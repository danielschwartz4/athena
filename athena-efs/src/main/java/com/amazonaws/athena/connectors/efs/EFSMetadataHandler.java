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
import com.amazonaws.athena.connector.lambda.data.projectors.ArrowValueProjectorImpl;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
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

import java.awt.desktop.SystemEventListener;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
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
    private EFSPathUtils efsPathUtils;
    private EFSTypeUtils typeUtils;
    private EFSValueReaderTypes valueReaderTypes;

    public EFSMetadataHandler()
    {
        super(false, SOURCE_TYPE);
        this.glueClient = getAwsGlue();
        this.efsPathUtils = new EFSPathUtils();
        this.valueReaderTypes = new EFSValueReaderTypes();
        this.typeUtils = new EFSTypeUtils();
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
        this.valueReaderTypes = new EFSValueReaderTypes();
        this.efsPathUtils = new EFSPathUtils();
        this.typeUtils = new EFSTypeUtils();
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
        combinedSchemas.add(DEFAULT_SCHEMA);
        return new ListSchemasResponse(request.getCatalogName(), combinedSchemas);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {
        Set<TableName> combinedTables = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                combinedTables.addAll(super.doListTables(allocator, request,
                        TABLE_FILTER).getTables());
            }
            catch (Exception e) {
                logger.warn("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables), null);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws Exception {
        logger.info("doGetTable: enter - " + request);
        Schema schema = null;
        Set<String> partitionColNames = Collections.emptySet();

        if (glueClient != null) {
            try {
                GetTableResponse table = super.doGetTable(allocator, request);
                schema = table.getSchema();
                partitionColNames = table.getPartitionColumns();
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

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                (schema == null) ? SchemaBuilder.newBuilder().build() : schema,
                partitionColNames);
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {
        Set<String> partitionCols = request.getPartitionCols();
        Set<String> resPaths = new HashSet();
        String d = System.getenv("EFS_PATH")
                + "/" + System.getenv("INPUT_TABLE");
        Path tablePath = Paths.get(d);

        Set<String> partitionSet = new HashSet();
        efsPathUtils.getDirectoriesDFS(tablePath.toFile().listFiles(), "", resPaths, partitionSet);

        if (!partitionCols.isEmpty()) {
            for (String path : resPaths) {
                if (!path.isEmpty()) {
                    String[] dirs = path.split("/");
                    blockWriter.writeRows((Block block, int row) -> {
                        boolean matched = true;
                        for (String dir : dirs) {
                            if (!dir.isEmpty() && dir.contains("=")) {
                                String[] dirParts = dir.split("=");
                                String col = dirParts[0];
                                Object val = typeUtils.typeParser(
                                        block.getFieldReader(col).getField(),
                                        dirParts[1]);
                                if (partitionCols.contains(col)) {
                                    matched &= block.setValue(col, row, val);
                                }
                            }
                        }
                        return matched ? 1 : 0;
                    });
                }
            }
        } else {
            blockWriter.writeRows((Block block, int row) -> {
                System.out.println("HERE");
                String col = "partitionId";
                Object val = typeUtils.typeParser(
                        block.getFieldReader(col).getField(),
                        "1");
                System.out.println(col);
                System.out.println(val);
                block.setValue(col, row, val);
                return 1;
            });
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws IOException {
        logger.info("doGetSplits: enter - " + request);
        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        List<FieldReader> fieldReaders = partitions.getFieldReaders();
        int rowCount = partitions.getRowCount();
        Set<String> partitionSet = new HashSet<>();

//      Convert partitions from fieldReader format to "col=val" format and put them in a set
        if (partitions.getFieldReaders().size() > 1) {
            for (int i = 0; i < rowCount; i++) {
                for (FieldReader locationReader : fieldReaders) {
                    locationReader.setPosition(i);
                    String fieldName = locationReader.getField().getName();
                    String val = valueReaderTypes.convertType(locationReader);
                    if (!Objects.equals(val, "null")) {
                        partitionSet.add(fieldName + "=" + val);
                    }
                }
            }
        }

        String pathString = System.getenv("EFS_PATH") + "/"
                + System.getenv("INPUT_TABLE");

//      Get all file paths that belong to the partition set from above. If there no partitions, we just get all files
        Set<String> resPaths = new HashSet<>();
        efsPathUtils.getDirectoriesDFS(Objects.requireNonNull(Paths.get(pathString).toFile().listFiles()), "", resPaths, partitionSet);

//      Make a split for each file
        int index = 0;
        for (String p : resPaths) {
            Split.Builder splitBuilder = Split.newBuilder(this.makeSpillLocation(request), this.makeEncryptionKey());
            splitBuilder.add(String.valueOf(index), pathString + p);
            Split split = splitBuilder.build();
            splits.add(split);
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }
}
