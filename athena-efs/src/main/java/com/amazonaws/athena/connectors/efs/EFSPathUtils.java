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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class EFSPathUtils {
    private final Path tablePath = Paths.get(System.getenv("EFS_PATH")
            + "/" + System.getenv("INPUT_TABLE"));


    protected Set<String> getDirectories() throws IOException {
        Set<String> directories = Files.walk(tablePath).filter(dir -> Files.isDirectory(dir))
//                .filter(dir -> !Objects.equals(dir.toString(), System.getenv("INPUT_TABLE")))
                .map(Path::getFileName)
                .map(Path::toString)
                .collect(Collectors.toSet());

        return directories;
    }
}
