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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlRewriterUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Helper class for converting {@link SqlCreateTable} to {@link CreateTableOperation}. */
class SqlCreateTableConverter {

    private final MergeTableLikeUtil mergeTableLikeUtil;
    private final MergeTableAsUtil mergeTableAsUtil;
    private final CatalogManager catalogManager;
    private final FlinkTypeFactory typeFactory;
    private final SqlRewriterUtils rewriterUtils;

    SqlCreateTableConverter(
            FlinkCalciteSqlValidator sqlValidator,
            CatalogManager catalogManager,
            Function<SqlNode, String> escapeExpression) {
        this.mergeTableLikeUtil =
                new MergeTableLikeUtil(
                        sqlValidator, escapeExpression, catalogManager.getDataTypeFactory());
        this.mergeTableAsUtil =
                new MergeTableAsUtil(
                        sqlValidator, escapeExpression, catalogManager.getDataTypeFactory());
        this.catalogManager = catalogManager;
        this.typeFactory = (FlinkTypeFactory) sqlValidator.getTypeFactory();
        this.rewriterUtils = new SqlRewriterUtils(sqlValidator);
    }

    /** Convert the {@link SqlCreateTable} node. */
    Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
        ResolvedCatalogTable catalogTable = createCatalogTable(sqlCreateTable);

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateTable.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new CreateTableOperation(
                identifier,
                catalogTable,
                sqlCreateTable.isIfNotExists(),
                sqlCreateTable.isTemporary());
    }

    /** Convert the {@link SqlCreateTableAs} node. */
    Operation convertCreateTableAS(
            FlinkPlannerImpl flinkPlanner, SqlCreateTableAs sqlCreateTableAs) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateTableAs.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        SqlNode asQuerySqlNode = sqlCreateTableAs.getAsQuery();
        SqlNode validatedAsQuery = flinkPlanner.validate(asQuerySqlNode);

        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        SqlNodeToOperationConversion.convert(
                                        flinkPlanner, catalogManager, validatedAsQuery)
                                .orElseThrow(
                                        () ->
                                                new TableException(
                                                        "CTAS unsupported node type "
                                                                + validatedAsQuery
                                                                        .getClass()
                                                                        .getSimpleName()));
        ResolvedCatalogTable tableWithResolvedSchema =
                createCatalogTable(sqlCreateTableAs, query.getResolvedSchema());

        // If needed, rewrite the query to include the new sink fields in the select list
        query =
                mergeTableAsUtil.maybeRewriteQuery(
                        catalogManager,
                        flinkPlanner,
                        query,
                        validatedAsQuery,
                        tableWithResolvedSchema);

        CreateTableOperation createTableOperation =
                new CreateTableOperation(
                        identifier,
                        tableWithResolvedSchema,
                        sqlCreateTableAs.isIfNotExists(),
                        sqlCreateTableAs.isTemporary());

        return new CreateTableASOperation(
                createTableOperation, Collections.emptyMap(), query, false);
    }

    private ResolvedCatalogTable createCatalogTable(
            SqlCreateTableAs sqlCreateTableAs, ResolvedSchema querySchema) {
        Map<String, String> tableOptions =
                sqlCreateTableAs.getPropertyList().getList().stream()
                        .collect(
                                Collectors.toMap(
                                        p -> ((SqlTableOption) p).getKeyString(),
                                        p -> ((SqlTableOption) p).getValueString()));

        String tableComment =
                OperationConverterUtils.getTableComment(sqlCreateTableAs.getComment());

        Schema mergedSchema;
        if (sqlCreateTableAs.isSchemaWithColumnsIdentifiersOnly()) {
            // If only column identifiers are provided, then these are used to
            // order the columns in the schema.
            mergedSchema =
                    mergeTableAsUtil.reorderSchema(sqlCreateTableAs.getColumnList(), querySchema);
        } else {
            mergedSchema =
                    mergeTableAsUtil.mergeSchemas(
                            sqlCreateTableAs.getColumnList(),
                            sqlCreateTableAs.getWatermark().orElse(null),
                            sqlCreateTableAs.getFullConstraints(),
                            querySchema);
        }

        Optional<TableDistribution> tableDistribution =
                Optional.ofNullable(sqlCreateTableAs.getDistribution())
                        .map(OperationConverterUtils::getDistributionFromSqlDistribution);

        List<String> partitionKeys =
                getPartitionKeyColumnNames(sqlCreateTableAs.getPartitionKeyList());
        verifyPartitioningColumnsExist(mergedSchema, partitionKeys);

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(mergedSchema)
                        .comment(tableComment)
                        .distribution(tableDistribution.orElse(null))
                        .options(tableOptions)
                        .partitionKeys(partitionKeys)
                        .build();

        return catalogManager.resolveCatalogTable(catalogTable);
    }

    private ResolvedCatalogTable createCatalogTable(SqlCreateTable sqlCreateTable) {

        final Schema sourceTableSchema;
        final Optional<TableDistribution> sourceTableDistribution;
        final List<String> sourcePartitionKeys;
        final List<SqlTableLike.SqlTableLikeOption> likeOptions;
        final Map<String, String> sourceProperties;
        if (sqlCreateTable instanceof SqlCreateTableLike) {
            SqlTableLike sqlTableLike = ((SqlCreateTableLike) sqlCreateTable).getTableLike();
            CatalogTable table = lookupLikeSourceTable(sqlTableLike);
            sourceTableSchema = table.getUnresolvedSchema();
            sourceTableDistribution = table.getDistribution();
            sourcePartitionKeys = table.getPartitionKeys();
            likeOptions = sqlTableLike.getOptions();
            sourceProperties = table.getOptions();
        } else {
            sourceTableSchema = Schema.newBuilder().build();
            sourceTableDistribution = Optional.empty();
            sourcePartitionKeys = Collections.emptyList();
            likeOptions = Collections.emptyList();
            sourceProperties = Collections.emptyMap();
        }

        Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies =
                mergeTableLikeUtil.computeMergingStrategies(likeOptions);

        Map<String, String> mergedOptions =
                mergeOptions(sqlCreateTable, sourceProperties, mergingStrategies);

        // It is assumed only a primary key constraint may be defined in the table. The
        // SqlCreateTableAs has validations to ensure this before the object is created.
        Optional<SqlTableConstraint> primaryKey =
                sqlCreateTable.getFullConstraints().stream()
                        .filter(SqlTableConstraint::isPrimaryKey)
                        .findAny();

        Schema mergedSchema =
                mergeTableLikeUtil.mergeTables(
                        mergingStrategies,
                        sourceTableSchema,
                        sqlCreateTable.getColumnList().getList(),
                        sqlCreateTable
                                .getWatermark()
                                .map(Collections::singletonList)
                                .orElseGet(Collections::emptyList),
                        primaryKey.orElse(null));

        Optional<TableDistribution> mergedTableDistribution =
                mergeDistribution(sourceTableDistribution, sqlCreateTable, mergingStrategies);

        List<String> partitionKeys =
                mergePartitions(
                        sourcePartitionKeys,
                        sqlCreateTable.getPartitionKeyList(),
                        mergingStrategies);
        verifyPartitioningColumnsExist(mergedSchema, partitionKeys);

        String tableComment = OperationConverterUtils.getTableComment(sqlCreateTable.getComment());

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(mergedSchema)
                        .comment(tableComment)
                        .distribution(mergedTableDistribution.orElse(null))
                        .options(new HashMap<>(mergedOptions))
                        .partitionKeys(partitionKeys)
                        .build();

        return catalogManager.resolveCatalogTable(catalogTable);
    }

    private CatalogTable lookupLikeSourceTable(SqlTableLike sqlTableLike) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlTableLike.getSourceTable().names);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        ContextResolvedTable lookupResult =
                catalogManager
                        .getTable(identifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Source table '%s' of the LIKE clause not found in the catalog, at %s",
                                                        identifier,
                                                        sqlTableLike
                                                                .getSourceTable()
                                                                .getParserPosition())));
        if (!(lookupResult.getResolvedTable() instanceof CatalogTable)) {
            throw new ValidationException(
                    String.format(
                            "Source table '%s' of the LIKE clause can not be a VIEW, at %s",
                            identifier, sqlTableLike.getSourceTable().getParserPosition()));
        }
        return lookupResult.getResolvedTable();
    }

    private void verifyPartitioningColumnsExist(Schema mergedSchema, List<String> partitionKeys) {
        Set<String> columnNames =
                mergedSchema.getColumns().stream()
                        .map(Schema.UnresolvedColumn::getName)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String partitionKey : partitionKeys) {
            if (!columnNames.contains(partitionKey)) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the table schema. Available columns: [%s]",
                                partitionKey,
                                columnNames.stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }
    }

    private Optional<TableDistribution> mergeDistribution(
            Optional<TableDistribution> sourceTableDistribution,
            SqlCreateTable sqlCreateTable,
            Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies) {

        Optional<TableDistribution> derivedTabledDistribution = Optional.empty();
        if (sqlCreateTable.getDistribution() != null) {
            TableDistribution distribution =
                    OperationConverterUtils.getDistributionFromSqlDistribution(
                            sqlCreateTable.getDistribution());
            derivedTabledDistribution = Optional.of(distribution);
        }

        return mergeTableLikeUtil.mergeDistribution(
                mergingStrategies.get(SqlTableLike.FeatureOption.DISTRIBUTION),
                sourceTableDistribution,
                derivedTabledDistribution);
    }

    private List<String> getPartitionKeyColumnNames(SqlNodeList partitionKey) {
        return partitionKey.getList().stream()
                .map(p -> ((SqlIdentifier) p).getSimple())
                .collect(Collectors.toList());
    }

    private List<String> mergePartitions(
            List<String> sourcePartitionKeys,
            SqlNodeList derivedPartitionKeys,
            Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies) {
        // set partition key
        return mergeTableLikeUtil.mergePartitions(
                mergingStrategies.get(SqlTableLike.FeatureOption.PARTITIONS),
                sourcePartitionKeys,
                getPartitionKeyColumnNames(derivedPartitionKeys));
    }

    private Map<String, String> mergeOptions(
            SqlCreateTable sqlCreateTable,
            Map<String, String> sourceProperties,
            Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies) {
        // set with properties
        Map<String, String> properties = new HashMap<>();
        sqlCreateTable
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));
        return mergeTableLikeUtil.mergeOptions(
                mergingStrategies.get(SqlTableLike.FeatureOption.OPTIONS),
                sourceProperties,
                properties);
    }
}
