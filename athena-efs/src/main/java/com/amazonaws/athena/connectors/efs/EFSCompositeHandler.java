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

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class EFSCompositeHandler extends CompositeHandler {
    public EFSCompositeHandler() {
        super(new EFSMetadataHandler(), new EFSRecordHandler(), new EFSUserDefinedFuncHandler());
        this.test();
    }

    public void test() {
        System.out.println("EFS console log test from Composite Handler");
    }
}
