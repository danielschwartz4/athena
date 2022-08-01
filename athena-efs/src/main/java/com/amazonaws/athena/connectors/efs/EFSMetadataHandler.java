/*-
 * #%L
 * athena-efs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.amazonaws.athena.connectors.efs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EFSMetadataHandler
        extends GlueMetadataHandler
{
    private static final String SOURCE_TYPE = "efs";
    protected static final String EFS_DB_FLAG = "efs-db-flag";
    private static final Logger logger = LoggerFactory.getLogger(EFSMetadataHandler.class);
    private static final GlueMetadataHandler.DatabaseFilter DB_FILTER = (Database database) -> 2 == 2;
    private static final TableFilter TABLE_FILTER = (Table table) -> 2 == 2 ;
    private final AWSGlue glueClient;
    public static final String DEFAULT_SCHEMA = "default";
    public EFSMetadataHandler()
    {
        super(false, SOURCE_TYPE);
        this.glueClient = getAwsGlue();
    }


    @VisibleForTesting
    protected EFSMetadataHandler(EncryptionKeyFactory keyFactory,
                                 AWSSecretsManager awsSecretsManager,
                                 AmazonAthena athena,
                                 String spillBucket,
                                 String spillPrefix,
                                 AWSGlue glueClient)
    {
        super(glueClient, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.glueClient = glueClient;
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);
        Set<String> combinedSchemas = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                combinedSchemas.addAll(super.doListSchemaNames(allocator, request, DB_FILTER).getSchemas());
            }
            catch (RuntimeException e) {
                logger.warn("doListSchemaNames: Unable to retrieve schemas from AWSGlue.", e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("SCHEMAAS");
        System.out.println(combinedSchemas);
        combinedSchemas.add(DEFAULT_SCHEMA);
        return new ListSchemasResponse(request.getCatalogName(), combinedSchemas);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {
        // LinkedHashSet for consistent ordering
        Set<TableName> combinedTables = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                combinedTables.addAll(super.doListTables(allocator, request,
                        //                        new ListTablesRequest(request.getIdentity(), request.getQueryId(), request.getCatalogName(),
                        //                                request.getSchemaName(), null, UNLIMITED_PAGE_SIZE_VALUE),
                        TABLE_FILTER).getTables());
            }
            catch (Exception e) {
                logger.warn("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }
        System.out.println("COMBINED TABLES");
        System.out.println(combinedTables);
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables), null);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws Exception {
        logger.info("doGetTable: enter - " + request);
        Schema schema = null;

        if (glueClient != null) {
            try {
                schema = super.doGetTable(allocator, request).getSchema();
            }
            catch (RuntimeException e) {
                logger.warn("doGetTable: Unable to retrieve table {} from AWSGlue in database/schema {}. " +
                                "Falling back to schema inference. If inferred schema is incorrect, create " +
                                "a matching table in Glue to define schema (see README)",
                        request.getTableName().getTableName(), request.getTableName().getSchemaName(), e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("getCustomMetadata");
        System.out.println(schema.getCustomMetadata());
        System.out.println("FIELDS");
        System.out.println(schema.getFields());

        Set<String> partitionColNames = new HashSet<String>();
        for (int i = 0; i < schema.getFields().size(); i++) {
            String name = schema.getFields().get(i).getName();
            partitionColNames.add(name);
        }

        //        If glue not present, infer schema
        //        ...

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                (schema == null) ? SchemaBuilder.newBuilder().build() : schema, partitionColNames);
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {
        System.out.println("SUPER PARTITIONS");
        String tableName = getSourceTableName(request.getSchema());
        if (tableName == null) {
            tableName = request.getTableName().getTableName();
        }
        Set<String> partitionCols = request.getPartitionCols();
        Iterator<String> itr = partitionCols.iterator();
        System.out.println("PARTITIONCOLS: " + partitionCols);

        blockWriter.writeRows((Block block, int row) -> {
            boolean matched = true;
            int i = 0;
            while(itr.hasNext()) {
                String val = itr.next();
                System.out.println("VAL: " + val);
                matched &= block.setValue(val, row, partitionCols.size() - i);
                i++;
            }
            return matched ? 1 : 0;
        });
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {
        logger.info("doGetSplits: enter - " + request);
        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet();
        Block partitions = request.getPartitions();
        Map<String, String> partitionMetadata = partitions.getSchema().getCustomMetadata();
        Split.Builder splitBuilder = Split.newBuilder(this.makeSpillLocation(request), this.makeEncryptionKey());
        if (partitions.toString() != "{}") {
            Split split = splitBuilder.build();
            splits.add(split);
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }
}
