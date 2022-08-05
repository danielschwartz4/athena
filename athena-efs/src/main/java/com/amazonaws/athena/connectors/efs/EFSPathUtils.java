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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class EFSPathUtils {
    private String tmpPath;

    public EFSPathUtils() {
        this.tmpPath = "";
    }
    private final Path tablePath = Paths.get(System.getenv("EFS_PATH")
            + "/" + System.getenv("INPUT_TABLE"));

    protected Set<String> getDirectories() throws IOException {
        Set<String> directories = Files.walk(tablePath).filter(dir -> Files.isDirectory(dir))
                .map(Path::getFileName)
                .map(Path::toString)
                .collect(Collectors.toSet());

        return directories;
    }
    protected void getDirectoriesDFS(File[] files, String tmpFile, Set<String> resFiles) throws IOException {
        for (File filename : files) {
            if (filename.isDirectory()) {
                this.tmpPath += "/" + filename.getName();
                getDirectoriesDFS(filename.listFiles(), this.tmpPath, resFiles);
            }
            else {
                System.out.println("File: " + filename.getName());
            }
        }
        if (tmpFile != "") {
            resFiles.add(tmpFile);
        }
        try {
            int index = this.tmpPath.lastIndexOf("/");
            this.tmpPath = this.tmpPath.substring(0, index);
        } catch(Exception e) {
            this.tmpPath = "";
        }
    }
}
