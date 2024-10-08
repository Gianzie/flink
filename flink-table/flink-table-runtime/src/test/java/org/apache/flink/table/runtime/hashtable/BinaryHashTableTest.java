/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.ConstantsKeyValuePairsIterator;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Hash table it case for binary row. */
@ExtendWith(ParameterizedTestExtension.class)
class BinaryHashTableTest {

    private static final int PAGE_SIZE = 32 * 1024;
    private IOManager ioManager;
    private BinaryRowDataSerializer buildSideSerializer;
    private BinaryRowDataSerializer probeSideSerializer;

    private boolean useCompress;

    BinaryHashTableTest(boolean useCompress) {
        this.useCompress = useCompress;
    }

    @Parameters(name = "useCompress-{0}")
    private static List<Boolean> getVarSeg() {
        return Arrays.asList(true, false);
    }

    @BeforeEach
    void setup() {
        TypeInformation[] types = new TypeInformation[] {Types.INT, Types.INT};
        this.buildSideSerializer = new BinaryRowDataSerializer(types.length);
        this.probeSideSerializer = new BinaryRowDataSerializer(types.length);

        this.ioManager = new IOManagerAsync();
    }

    @AfterEach
    void tearDown() throws Exception {
        // shut down I/O manager and Memory Manager and verify the correct shutdown
        this.ioManager.close();
    }

    @TestTemplate
    void testIOBufferCountComputation() {
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(32)).isEqualTo(1);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(33)).isEqualTo(1);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(40)).isEqualTo(1);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(64)).isEqualTo(1);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(127)).isEqualTo(1);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(128)).isEqualTo(2);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(129)).isEqualTo(2);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(511)).isEqualTo(2);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(512)).isEqualTo(3);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(513)).isEqualTo(3);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(2047)).isEqualTo(3);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(2048)).isEqualTo(4);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(2049)).isEqualTo(4);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(8191)).isEqualTo(4);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(8192)).isEqualTo(5);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(8193)).isEqualTo(5);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(32767)).isEqualTo(5);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(32768)).isEqualTo(6);
        assertThat(BinaryHashTable.getNumWriteBehindBuffers(Integer.MAX_VALUE)).isEqualTo(6);
    }

    @TestTemplate
    void testInMemoryMutableHashTable() throws IOException {
        final int numKeys = 100000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
        // ----------------------------------------------------------------------------------------
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        100 * PAGE_SIZE,
                        ioManager);

        int numRecordsInJoinResult = join(table, buildInput, probeInput);
        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(numKeys * buildValsPerKey * probeValsPerKey);

        table.close();

        table.free();
    }

    private int join(
            BinaryHashTable table,
            MutableObjectIterator<BinaryRowData> buildInput,
            MutableObjectIterator<BinaryRowData> probeInput)
            throws IOException {
        return join(table, buildInput, probeInput, false);
    }

    private int join(
            BinaryHashTable table,
            MutableObjectIterator<BinaryRowData> buildInput,
            MutableObjectIterator<BinaryRowData> probeInput,
            boolean buildOuterJoin)
            throws IOException {
        int count = 0;

        BinaryRowData reuseBuildSizeRow = buildSideSerializer.createInstance();
        BinaryRowData buildRow;
        while ((buildRow = buildInput.next(reuseBuildSizeRow)) != null) {
            table.putBuildRow(buildRow);
        }
        table.endBuild();

        BinaryRowData probeRow = probeSideSerializer.createInstance();
        while ((probeRow = probeInput.next(probeRow)) != null) {
            if (table.tryProbe(probeRow)) {
                count += joinWithNextKey(table, buildOuterJoin);
            }
        }

        while (table.nextMatching()) {
            count += joinWithNextKey(table, buildOuterJoin);
        }
        return count;
    }

    private int joinWithNextKey(BinaryHashTable table, boolean buildOuterJoin) throws IOException {
        int count = 0;
        final RowIterator<BinaryRowData> buildIterator = table.getBuildSideIterator();
        final RowData probeRow = table.getCurrentProbeRow();
        BinaryRowData buildRow;

        buildRow = buildIterator.advanceNext() ? buildIterator.getRow() : null;
        // get the first build side value
        if (probeRow != null && buildRow != null) {
            count++;
            while (buildIterator.advanceNext()) {
                count++;
            }
        } else {
            if (buildOuterJoin && probeRow == null && buildRow != null) {
                count++;
                while (buildIterator.advanceNext()) {
                    count++;
                }
            }
        }
        return count;
    }

    @TestTemplate
    void testSpillingHashJoinOneRecursionPerformance() throws IOException {
        final int numKeys = 1000000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        // allocate the memory for the HashTable
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(200 * PAGE_SIZE)
                        .setPageSize(PAGE_SIZE)
                        .build();
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        100 * PAGE_SIZE,
                        ioManager);

        // ----------------------------------------------------------------------------------------

        int numRecordsInJoinResult = join(table, buildInput, probeInput);

        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(numKeys * buildValsPerKey * probeValsPerKey);

        table.close();

        // ----------------------------------------------------------------------------------------

        table.free();
    }

    @TestTemplate
    void testSpillingHashJoinOneRecursionValidity() throws IOException {
        final int numKeys = 1000000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<>(numKeys);

        // ----------------------------------------------------------------------------------------
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        100 * PAGE_SIZE,
                        ioManager);
        final BinaryRowData recordReuse = new BinaryRowData(2);

        BinaryRowData buildRow = buildSideSerializer.createInstance();
        while ((buildRow = buildInput.next(buildRow)) != null) {
            table.putBuildRow(buildRow);
        }
        table.endBuild();

        BinaryRowData probeRow = probeSideSerializer.createInstance();
        while ((probeRow = probeInput.next(probeRow)) != null) {
            if (table.tryProbe(probeRow)) {
                testJoin(table, map);
            }
        }

        while (table.nextMatching()) {
            testJoin(table, map);
        }

        table.close();

        assertThat(map).as("Wrong number of keys").hasSize(numKeys);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .as("Wrong number of values in per-key cross product for key " + key)
                    .isEqualTo(probeValsPerKey * buildValsPerKey);
        }

        // ----------------------------------------------------------------------------------------

        table.free();
    }

    @TestTemplate
    void testSpillingHashJoinWithMassiveCollisions() throws IOException {
        // the following two values are known to have a hash-code collision on the initial level.
        // we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 200000;
        final int repeatedValueCountProbe = 5;

        final int numKeys = 1000000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<BinaryRowData> probe1 =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probe2 =
                new ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
        MutableObjectIterator<BinaryRowData> probe3 =
                new ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
        List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<>(numKeys);
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
        // ----------------------------------------------------------------------------------------

        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        896 * PAGE_SIZE,
                        ioManager);

        final BinaryRowData recordReuse = new BinaryRowData(2);

        BinaryRowData buildRow = buildSideSerializer.createInstance();
        while ((buildRow = buildInput.next(buildRow)) != null) {
            table.putBuildRow(buildRow);
        }
        table.endBuild();

        BinaryRowData probeRow = probeSideSerializer.createInstance();
        while ((probeRow = probeInput.next(probeRow)) != null) {
            if (table.tryProbe(probeRow)) {
                testJoin(table, map);
            }
        }

        while (table.nextMatching()) {
            testJoin(table, map);
        }

        table.close();

        assertThat(map).as("Wrong number of keys").hasSize(numKeys);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .as("Wrong number of values in per-key cross product for key " + key)
                    .isEqualTo(
                            (key == repeatedValue1 || key == repeatedValue2)
                                    ? (probeValsPerKey + repeatedValueCountProbe)
                                            * (buildValsPerKey + repeatedValueCountBuild)
                                    : probeValsPerKey * buildValsPerKey);
        }

        // ----------------------------------------------------------------------------------------

        table.free();
    }

    private void testJoin(BinaryHashTable table, HashMap<Integer, Long> map) throws IOException {
        BinaryRowData record;
        int numBuildValues = 0;

        final RowData probeRec = table.getCurrentProbeRow();
        int key = probeRec.getInt(0);

        RowIterator<BinaryRowData> buildSide = table.getBuildSideIterator();
        if (buildSide.advanceNext()) {
            numBuildValues = 1;
            record = buildSide.getRow();
            assertThat(record.getInt(0))
                    .as("Probe-side key was different than build-side key.")
                    .isEqualTo(key);
        } else {
            fail("No build side values found for a probe key.");
        }
        while (buildSide.advanceNext()) {
            numBuildValues++;
            record = buildSide.getRow();
            assertThat(record.getInt(0))
                    .as("Probe-side key was different than build-side key.")
                    .isEqualTo(key);
        }

        Long contained = map.get(key);
        if (contained == null) {
            contained = (long) numBuildValues;
        } else {
            contained = contained + numBuildValues;
        }

        map.put(key, contained);
    }

    /*
     * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
     * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
     * fits into memory by itself and needs to be repartitioned in the recursion again.
     */
    @TestTemplate
    void testSpillingHashJoinWithTwoRecursions() throws IOException {
        // the following two values are known to have a hash-code collision on the first recursion
        // level.
        // we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 200000;
        final int repeatedValueCountProbe = 5;

        final int numKeys = 1000000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<BinaryRowData> probe1 =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probe2 =
                new ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
        MutableObjectIterator<BinaryRowData> probe3 =
                new ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
        List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<>(numKeys);

        // ----------------------------------------------------------------------------------------
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        896 * PAGE_SIZE,
                        ioManager);
        final BinaryRowData recordReuse = new BinaryRowData(2);

        BinaryRowData buildRow = buildSideSerializer.createInstance();
        while ((buildRow = buildInput.next(buildRow)) != null) {
            table.putBuildRow(buildRow);
        }
        table.endBuild();

        BinaryRowData probeRow = probeSideSerializer.createInstance();
        while ((probeRow = probeInput.next(probeRow)) != null) {
            if (table.tryProbe(probeRow)) {
                testJoin(table, map);
            }
        }

        while (table.nextMatching()) {
            testJoin(table, map);
        }

        table.close();

        assertThat(map).as("Wrong number of keys").hasSize(numKeys);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .as("Wrong number of values in per-key cross product for key " + key)
                    .isEqualTo(
                            (key == repeatedValue1 || key == repeatedValue2)
                                    ? (probeValsPerKey + repeatedValueCountProbe)
                                            * (buildValsPerKey + repeatedValueCountBuild)
                                    : probeValsPerKey * buildValsPerKey);
        }

        // ----------------------------------------------------------------------------------------

        table.free();
    }

    /*
     * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
     * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
     * fits into memory by itself and needs to be repartitioned in the recursion again.
     */
    @TestTemplate
    void testSpillingHashJoinWithTooManyRecursions() throws IOException {
        // the following two values are known to have a hash-code collision on the first recursion
        // level.
        // we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCount = 3000000;

        final int numKeys = 1000000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCount);
        MutableObjectIterator<BinaryRowData> build3 =
                new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCount);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<BinaryRowData> probe1 =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probe2 =
                new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCount);
        MutableObjectIterator<BinaryRowData> probe3 =
                new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCount);
        List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);
        // ----------------------------------------------------------------------------------------
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        896 * PAGE_SIZE,
                        ioManager);

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<>(numKeys);

        BinaryRowData buildRow = buildSideSerializer.createInstance();
        while ((buildRow = buildInput.next(buildRow)) != null) {
            table.putBuildRow(buildRow);
        }
        table.endBuild();

        BinaryRowData probeRow = probeSideSerializer.createInstance();
        while ((probeRow = probeInput.next(probeRow)) != null) {
            if (table.tryProbe(probeRow)) {
                testJoin(table, map);
            }
        }

        while (table.nextMatching()) {
            testJoin(table, map);
        }

        // The partition which spill to disk more than 3 can't be joined
        assertThat(map.size()).as("Wrong number of records in join result.").isLessThan(numKeys);

        // Here exists two partition which spill to disk more than 3
        assertThat(table.getPartitionsPendingForSMJ().size())
                .as("Wrong number of spilled partition.")
                .isEqualTo(2);

        Map<Integer, Integer> spilledPartitionBuildSideKeys = new HashMap<>();
        Map<Integer, Integer> spilledPartitionProbeSideKeys = new HashMap<>();
        for (BinaryHashPartition p : table.getPartitionsPendingForSMJ()) {
            RowIterator<BinaryRowData> buildIter = table.getSpilledPartitionBuildSideIter(p);
            while (buildIter.advanceNext()) {
                Integer key = buildIter.getRow().getInt(0);
                spilledPartitionBuildSideKeys.put(
                        key, spilledPartitionBuildSideKeys.getOrDefault(key, 0) + 1);
            }

            ProbeIterator probeIter = table.getSpilledPartitionProbeSideIter(p);
            BinaryRowData rowData;
            while ((rowData = probeIter.next()) != null) {
                Integer key = rowData.getInt(0);
                spilledPartitionProbeSideKeys.put(
                        key, spilledPartitionProbeSideKeys.getOrDefault(key, 0) + 1);
            }
        }

        // assert spilled partition contains key repeatedValue1 and repeatedValue2
        Integer buildKeyCnt = repeatedValueCount + buildValsPerKey;
        assertThat(spilledPartitionBuildSideKeys).containsEntry(repeatedValue1, buildKeyCnt);
        assertThat(spilledPartitionBuildSideKeys).containsEntry(repeatedValue2, buildKeyCnt);

        Integer probeKeyCnt = repeatedValueCount + probeValsPerKey;
        assertThat(spilledPartitionProbeSideKeys).containsEntry(repeatedValue1, probeKeyCnt);
        assertThat(spilledPartitionProbeSideKeys).containsEntry(repeatedValue2, probeKeyCnt);

        table.close();

        // ----------------------------------------------------------------------------------------

        table.free();
    }

    /*
     * Spills build records, so that probe records are also spilled. But only so
     * few probe records are used that some partitions remain empty.
     */
    @TestTemplate
    void testSparseProbeSpilling() throws IOException, MemoryAllocationException {
        final int numBuildKeys = 1000000;
        final int numBuildVals = 1;
        final int numProbeKeys = 20;
        final int numProbeVals = 1;

        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numBuildKeys, numBuildVals, false);
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(128 * PAGE_SIZE).build();
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        100 * PAGE_SIZE,
                        ioManager);

        int expectedNumResults =
                (Math.min(numProbeKeys, numBuildKeys) * numBuildVals) * numProbeVals;

        int numRecordsInJoinResult =
                join(
                        table,
                        buildInput,
                        new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true));

        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        table.close();

        table.free();
    }

    /*
     * Same test as {@link #testSparseProbeSpilling} but using a build-side outer join
     * that requires spilled build-side records to be returned and counted.
     */
    @TestTemplate
    void testSparseProbeSpillingWithOuterJoin() throws IOException {
        final int numBuildKeys = 1000000;
        final int numBuildVals = 1;
        final int numProbeKeys = 20;
        final int numProbeVals = 1;

        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numBuildKeys, numBuildVals, false);
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(96 * PAGE_SIZE).build();
        final BinaryHashTable table =
                new BinaryHashTable(
                        new Object(),
                        useCompress,
                        (int) TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE.defaultValue().getBytes(),
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        96 * PAGE_SIZE,
                        ioManager,
                        24,
                        200000,
                        true,
                        HashJoinType.BUILD_OUTER,
                        null,
                        true,
                        new boolean[] {true},
                        false);

        int expectedNumResults =
                (Math.max(numProbeKeys, numBuildKeys) * numBuildVals) * numProbeVals;

        int numRecordsInJoinResult =
                join(
                        table,
                        buildInput,
                        new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true),
                        true);

        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        table.close();
        table.free();
    }

    /*
     * This test validates a bug fix against former memory loss in the case where a partition was spilled
     * during an insert into the same.
     */
    @TestTemplate
    void validateSpillingDuringInsertion() throws IOException {
        final int numBuildKeys = 500000;
        final int numBuildVals = 1;
        final int numProbeKeys = 10;
        final int numProbeVals = 1;

        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numBuildKeys, numBuildVals, false);
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(85 * PAGE_SIZE).build();
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        85 * PAGE_SIZE,
                        ioManager);

        int expectedNumResults =
                (Math.min(numProbeKeys, numBuildKeys) * numBuildVals) * numProbeVals;

        int numRecordsInJoinResult =
                join(
                        table,
                        buildInput,
                        new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true));

        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        table.close();

        table.free();
    }

    @TestTemplate
    void testBucketsNotFulfillSegment() throws Exception {
        final int numKeys = 10000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 10;

        // create a build input that gives 30000 pairs with 3 values sharing the same key
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

        // create a probe input that gives 100000 pairs with 10 values sharing a key
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        // allocate the memory for the HashTable
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
        // ----------------------------------------------------------------------------------------

        final BinaryHashTable table =
                new BinaryHashTable(
                        new Object(),
                        useCompress,
                        (int) TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE.defaultValue().getBytes(),
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        35 * PAGE_SIZE,
                        ioManager,
                        24,
                        200000,
                        true,
                        HashJoinType.INNER,
                        null,
                        false,
                        new boolean[] {true},
                        false);

        // For FLINK-2545, the buckets data may not fulfill it's buffer, for example, the buffer may
        // contains 256 buckets,
        // while hash table only assign 250 bucket on it. The unused buffer bytes may contains
        // arbitrary data, which may
        // influence hash table if forget to skip it. To mock this, put the invalid bucket
        // data(partition=1, inMemory=true, count=-1)
        // at the end of buffer.
        int totalPages = table.getInternalPool().freePages();
        for (int i = 0; i < totalPages; i++) {
            MemorySegment segment = table.getInternalPool().nextSegment();
            int newBucketOffset = segment.size() - 128;
            // initialize the header fields
            segment.put(newBucketOffset, (byte) 0);
            segment.put(newBucketOffset + 1, (byte) 0);
            segment.putShort(newBucketOffset + 2, (short) -1);
            segment.putLong(newBucketOffset + 4, ~0x0L);
            table.returnPage(segment);
        }

        int numRecordsInJoinResult = join(table, buildInput, probeInput);

        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(numKeys * buildValsPerKey * probeValsPerKey);

        table.close();
        table.free();
    }

    @TestTemplate
    void testHashWithBuildSideOuterJoin1() throws Exception {
        final int numKeys = 20000;
        final int buildValsPerKey = 1;
        final int probeValsPerKey = 1;

        // create a build input that gives 40000 pairs with 1 values sharing the same key
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(2 * numKeys, buildValsPerKey, false);

        // create a probe input that gives 20000 pairs with 1 values sharing a key
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
        // allocate the memory for the HashTable
        final BinaryHashTable table =
                new BinaryHashTable(
                        new Object(),
                        useCompress,
                        (int) TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE.defaultValue().getBytes(),
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        35 * PAGE_SIZE,
                        ioManager,
                        24,
                        200000,
                        true,
                        HashJoinType.BUILD_OUTER,
                        null,
                        true,
                        new boolean[] {true},
                        false);

        int numRecordsInJoinResult = join(table, buildInput, probeInput, true);

        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(2 * numKeys * buildValsPerKey * probeValsPerKey);

        table.close();
        table.free();
    }

    @TestTemplate
    void testHashWithBuildSideOuterJoin2() throws Exception {
        final int numKeys = 40000;
        final int buildValsPerKey = 2;
        final int probeValsPerKey = 1;

        // The keys of probe and build sides are overlapped, so there would be none unmatched build
        // elements
        // after probe phase, make sure build side outer join works well in this case.

        // create a build input that gives 80000 pairs with 2 values sharing the same key
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

        // create a probe input that gives 40000 pairs with 1 values sharing a key
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        // allocate the memory for the HashTable
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
        final BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        33 * PAGE_SIZE,
                        ioManager);

        // ----------------------------------------------------------------------------------------

        int numRecordsInJoinResult = join(table, buildInput, probeInput, true);
        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(numKeys * buildValsPerKey * probeValsPerKey);

        table.close();
        table.free();
    }

    @TestTemplate
    void testRepeatBuildJoin() throws Exception {
        final int numKeys = 500;
        final int probeValsPerKey = 1;
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(40 * PAGE_SIZE).build();
        MutableObjectIterator<BinaryRowData> buildInput =
                new MutableObjectIterator<BinaryRowData>() {

                    int cnt = 0;

                    @Override
                    public BinaryRowData next(BinaryRowData reuse) throws IOException {
                        return next();
                    }

                    @Override
                    public BinaryRowData next() {
                        cnt++;
                        if (cnt > numKeys) {
                            return null;
                        }
                        BinaryRowData row = new BinaryRowData(2);
                        BinaryRowWriter writer = new BinaryRowWriter(row);
                        writer.writeInt(0, 1);
                        writer.writeInt(1, 1);
                        writer.complete();
                        return row;
                    }
                };
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        final BinaryHashTable table =
                new BinaryHashTable(
                        new Object(),
                        useCompress,
                        (int) TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE.defaultValue().getBytes(),
                        buildSideSerializer,
                        probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        40 * PAGE_SIZE,
                        ioManager,
                        24,
                        200000,
                        true,
                        HashJoinType.INNER,
                        null,
                        false,
                        new boolean[] {true},
                        true);

        int numRecordsInJoinResult = join(table, buildInput, probeInput, true);
        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isEqualTo(1);

        table.close();
        table.free();
    }

    @TestTemplate
    void testRepeatBuildJoinWithSpill() throws Exception {
        final int numKeys = 30000;
        final int numRows = 300000;
        final int probeValsPerKey = 1;

        MutableObjectIterator<BinaryRowData> buildInput =
                new MutableObjectIterator<BinaryRowData>() {

                    int cnt = 0;

                    @Override
                    public BinaryRowData next(BinaryRowData reuse) throws IOException {
                        return next();
                    }

                    @Override
                    public BinaryRowData next() throws IOException {
                        cnt++;
                        if (cnt > numRows) {
                            return null;
                        }
                        int value = cnt % numKeys;
                        BinaryRowData row = new BinaryRowData(2);
                        BinaryRowWriter writer = new BinaryRowWriter(row);
                        writer.writeInt(0, value);
                        writer.writeInt(1, value);
                        writer.complete();
                        return row;
                    }
                };
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        final BinaryHashTable table =
                new BinaryHashTable(
                        new Object(),
                        useCompress,
                        (int) TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE.defaultValue().getBytes(),
                        buildSideSerializer,
                        probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        35 * PAGE_SIZE,
                        ioManager,
                        24,
                        200000,
                        true,
                        HashJoinType.INNER,
                        null,
                        false,
                        new boolean[] {true},
                        true);

        int numRecordsInJoinResult = join(table, buildInput, probeInput, true);
        assertThat(numRecordsInJoinResult)
                .as("Wrong number of records in join result.")
                .isLessThan(numRows);

        table.close();
        table.free();
    }

    @TestTemplate
    void testBinaryHashBucketAreaNotEnoughMem() throws IOException {
        MemoryManager memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
        BinaryHashTable table =
                newBinaryHashTable(
                        this.buildSideSerializer,
                        this.probeSideSerializer,
                        new MyProjection(),
                        new MyProjection(),
                        memManager,
                        35 * PAGE_SIZE,
                        ioManager);
        BinaryHashBucketArea area = new BinaryHashBucketArea(table, 100, 1, false);
        for (int i = 0; i < 100000; i++) {
            area.insertToBucket(i, i, true);
        }
        area.freeMemory();
        table.close();
        assertThat(table.getInternalPool().freePages()).isEqualTo(35);
    }

    // ============================================================================================

    private BinaryHashTable newBinaryHashTable(
            BinaryRowDataSerializer buildSideSerializer,
            BinaryRowDataSerializer probeSideSerializer,
            Projection<RowData, BinaryRowData> buildSideProjection,
            Projection<RowData, BinaryRowData> probeSideProjection,
            MemoryManager memoryManager,
            long memory,
            IOManager ioManager) {
        return new BinaryHashTable(
                new Object(),
                useCompress,
                (int) TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE.defaultValue().getBytes(),
                buildSideSerializer,
                probeSideSerializer,
                buildSideProjection,
                probeSideProjection,
                memoryManager,
                memory,
                ioManager,
                24,
                200000,
                true,
                HashJoinType.INNER,
                null,
                false,
                new boolean[] {true},
                false);
    }

    private static final class MyProjection implements Projection<RowData, BinaryRowData> {

        BinaryRowData innerRow = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(innerRow);

        @Override
        public BinaryRowData apply(RowData row) {
            writer.reset();
            if (row.isNullAt(0)) {
                writer.setNullAt(0);
            } else {
                writer.writeInt(0, row.getInt(0));
            }
            writer.complete();
            return innerRow;
        }
    }
}
