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

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.elasticfilesystem.AmazonElasticFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class TestBase {
    protected FederatedIdentity TEST_IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    protected static final String DEFAULT_SCHEMA = "default";

    protected static final String TEST_QUERY_ID = "queryId";
    protected static final String TEST_CATALOG_NAME = "default";
    protected static final String TEST_TABLE = "test_table";
    protected static final String TEST_TABLE2 = "Test_table2";
    protected static final String TEST_TABLE3 = "test_table3";
    protected static final String TEST_TABLE4 = "test_table4";
    protected static final TableName TEST_TABLE_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE);
    protected static final TableName TEST_TABLE_2_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE2);
    protected static final TableName TEST_TABLE_3_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE3);
    protected static final TableName TEST_TABLE_4_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE4);


//    protected static AmazonElasticFileSystem efsClient;

    @BeforeClass
    public static void setupOnce() throws Exception
    {
        seedFileSystem();
    }


    private static void seedFileSystem() throws InterruptedException
    {
        Path tablePath = Paths.get(System.getenv("EFS_PATH")
                + "/" + System.getenv("INPUT_TABLE"));
    }

//    @AfterClass
//    public static void tearDownOnce()
//    {
//        efsClient.shutdown();
//    }
}
