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
package com.amazonaws.athena.connectors.efs;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EFSMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(EFSMetadataHandlerTest.class);

    @Rule
    public TestName testName = new TestName();

    @Mock
    private AWSGlue glueClient;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    private EFSMetadataHandler handler;

    private BlockAllocator allocator;

    @Before
    public void setup()
    {
        logger.info("{}: enter", testName.getMethodName());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        allocator = new BlockAllocatorImpl();
        handler = new EFSMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", glueClient);
    }

    @Test
    public void doListSchemaNamesGlue()
            throws Exception
    {
        GetDatabasesResult result = new GetDatabasesResult().withDatabaseList(
                new Database().withName("testName"),
                new Database().withName("s3").withLocationUri("blah"));

        when(glueClient.getDatabases(any())).thenReturn(result);

        ListSchemasRequest req = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertThat(res.getSchemas().size(), equalTo(3));
        assertThat(res.getSchemas().contains("testName"), is(true));
        assertThat(res.getSchemas().contains("default"), is(true));
    }

    @Test
    public void doListTablesGlue()
            throws Exception
    {
        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");

        GetTablesResult mockResult = new GetTablesResult();
        List<Table> tableList = new ArrayList<>();
        tableList.add(new Table().withName("table1")
                .withParameters(ImmutableMap.of("classification", "efs"))
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("some.location")));
        tableList.add(new Table().withName("table2")
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("some.location")
                        .withParameters(ImmutableMap.of("classification", "efs"))));
        mockResult.setTableList(tableList);

        when(glueClient.getTables(any())).thenReturn(mockResult);

        ListTablesRequest req = new ListTablesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, DEFAULT_SCHEMA,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());

        List<TableName> expectedTables = tableNames.stream().map(table -> new TableName(DEFAULT_SCHEMA, table)).collect(Collectors.toList());

        assertThat(new HashSet<>(res.getTables()), equalTo(new HashSet<>(expectedTables)));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        com.amazonaws.athena.connector.lambda.metadata.GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE_NAME);
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());
        System.out.println(res.getTableName().getSchemaName());
        System.out.println(res.getTableName().getTableName());

        assertThat(res.getTableName().getSchemaName(), equalTo(DEFAULT_SCHEMA));
        assertThat(res.getTableName().getTableName(), equalTo(TEST_TABLE));
//        assertThat(res.getSchema().getFields().size(), equalTo(11));
    }


}
