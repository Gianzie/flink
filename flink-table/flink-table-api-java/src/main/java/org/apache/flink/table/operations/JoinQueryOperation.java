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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.operations.utils.OperationExpressionsUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Table operation that joins two relational operations based on given condition. */
@Internal
public class JoinQueryOperation implements QueryOperation {

    private static final String INPUT_1_ALIAS = "$$T1_JOIN";
    private static final String INPUT_2_ALIAS = "$$T2_JOIN";
    private final QueryOperation left;
    private final QueryOperation right;
    private final JoinType joinType;
    private final ResolvedExpression condition;
    private final boolean correlated;
    private final ResolvedSchema resolvedSchema;

    /** Specifies how the two Tables should be joined. */
    @Internal
    public enum JoinType {
        INNER,
        LEFT_OUTER,
        RIGHT_OUTER,
        FULL_OUTER
    }

    public JoinQueryOperation(
            QueryOperation left,
            QueryOperation right,
            JoinType joinType,
            ResolvedExpression condition,
            boolean correlated) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
        this.correlated = correlated;

        this.resolvedSchema = calculateResultingSchema(left, right);
    }

    private ResolvedSchema calculateResultingSchema(QueryOperation left, QueryOperation right) {
        final ResolvedSchema leftSchema = left.getResolvedSchema();
        final ResolvedSchema rightSchema = right.getResolvedSchema();
        return ResolvedSchema.physical(
                Stream.concat(
                                leftSchema.getColumnNames().stream(),
                                rightSchema.getColumnNames().stream())
                        .collect(Collectors.toList()),
                Stream.concat(
                                leftSchema.getColumnDataTypes().stream(),
                                rightSchema.getColumnDataTypes().stream())
                        .collect(Collectors.toList()));
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public ResolvedExpression getCondition() {
        return condition;
    }

    public boolean isCorrelated() {
        return correlated;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("joinType", joinType);
        args.put("condition", condition);
        args.put("correlated", correlated);

        return OperationUtils.formatWithChildren(
                "Join", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {

        Map<Integer, String> inputAliases = new HashMap<>();
        inputAliases.put(0, INPUT_1_ALIAS);
        inputAliases.put(
                1, correlated ? CorrelatedFunctionQueryOperation.INPUT_ALIAS : INPUT_2_ALIAS);

        return String.format(
                "SELECT %s FROM (%s\n) %s %s JOIN %s ON %s",
                getSelectList(),
                OperationUtils.indent(left.asSerializableString(sqlFactory)),
                INPUT_1_ALIAS,
                joinType.toString().replaceAll("_", " "),
                rightToSerializable(sqlFactory),
                OperationExpressionsUtils.scopeReferencesWithAlias(inputAliases, condition)
                        .asSerializableString(sqlFactory));
    }

    private String getSelectList() {
        String leftColumns =
                OperationUtils.formatSelectColumns(left.getResolvedSchema(), INPUT_1_ALIAS);
        String rightColumns =
                OperationUtils.formatSelectColumns(
                        right.getResolvedSchema(),
                        correlated ? CorrelatedFunctionQueryOperation.INPUT_ALIAS : INPUT_2_ALIAS);
        return leftColumns + ", " + rightColumns;
    }

    private String rightToSerializable(SqlFactory sqlFactory) {
        final StringBuilder s = new StringBuilder();
        if (!correlated) {
            s.append("(");
        }
        s.append(OperationUtils.indent(right.asSerializableString(sqlFactory)));
        if (!correlated) {
            s.append("\n)");
            s.append(" ");
            s.append(INPUT_2_ALIAS);
        }
        return s.toString();
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Arrays.asList(left, right);
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
