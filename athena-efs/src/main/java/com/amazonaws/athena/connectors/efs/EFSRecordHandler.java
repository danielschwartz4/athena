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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EFSRecordHandler extends RecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(EFSRecordHandler.class);
    private static final String SOURCE_TYPE = "efs";
    private EFSTypeUtils typeUtils;
    private AmazonS3 amazonS3;
    private EFSPathUtils efsPathUtils;

    public EFSRecordHandler() {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient());
        this.typeUtils = new EFSTypeUtils();
        this.efsPathUtils = new EFSPathUtils();
    }

    @VisibleForTesting
    protected EFSRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena) {
        super(amazonS3, secretsManager, amazonAthena, "efs");
        this.amazonS3 = amazonS3;
        this.typeUtils = new EFSTypeUtils();
        this.efsPathUtils = new EFSPathUtils();
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws IOException {
        Split split = recordsRequest.getSplit();
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

        Map<String, String> partitionValues = split.getProperties();

        Object[] partitionArr = partitionValues.entrySet().toArray();
        int arrSize = partitionArr.length;
        String pathString = System.getenv("EFS_PATH") + "/"
                + System.getenv("INPUT_TABLE");

        for (int i = arrSize-1; i >= 0; i--) {
            pathString += "/" + partitionArr[i];
        }
        Path path = Paths.get(pathString);

        int index = 0;

        for(Iterator itr = recordsRequest.getSchema().getFields().iterator(); itr.hasNext(); ++index) {
            Field next = (Field) itr.next();
            Extractor extractor = typeUtils.makeExtractor(next, index);
            if (extractor != null) {
                builder.withExtractor(next.getName(), extractor);
            }
        }

        GeneratedRowWriter rowWriter = builder.build();
        Set<String> resPaths = new HashSet();
        efsPathUtils.getDirectoriesDFS(path.toFile().listFiles(), "", resPaths);
        if (!resPaths.isEmpty()) {
            for (String p : resPaths) {
                if (!p.isEmpty()) {
                    String tmpDirPathString = pathString + p;
                    Path tmpDirPath = Paths.get(tmpDirPathString);

                    writeRows(spiller, tmpDirPath, rowWriter);
                }
            }
        } else {
            writeRows(spiller, path, rowWriter);
        }
    }
    private GeneratedRowWriter writeRows(BlockSpiller spiller, Path filePath, GeneratedRowWriter rowWriter) throws IOException {
        System.out.println("ROW WRITER file name: " + filePath.getFileName());
        Charset charset = StandardCharsets.UTF_8;
        String line;
        BufferedReader bufferedReader = Files.newBufferedReader(filePath, charset);
        while ((line = bufferedReader.readLine()) != null) {
            String[] lineParts = line.split(",");
            spiller.writeRows((block, rowNum) -> rowWriter.writeRow(block, rowNum, lineParts) ? 1 : 0);
        }
        return rowWriter;
    }
}
