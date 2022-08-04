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

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.projectors.ArrowValueProjectorImpl;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

public class EFSValueReaderTypes {
    protected String convertType(FieldReader fieldReader) {
        Types.MinorType minorType = fieldReader.getMinorType();
        switch (minorType) {
            case DATEMILLI: {
                if (Objects.isNull(fieldReader.readLocalDateTime())) {
                    return null;
                }
                long millis = fieldReader.readLocalDateTime().atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli();
                return String.valueOf(Instant.ofEpochMilli(millis).atZone(BlockUtils.UTC_ZONE_ID).toLocalDateTime());
            }
            case TINYINT:
            case UINT1:
                return String.valueOf(fieldReader.readByte());
            case UINT2:
                return String.valueOf(fieldReader.readCharacter());
            case SMALLINT:
                return String.valueOf(fieldReader.readShort());
            case DATEDAY: {
                    Integer intVal = fieldReader.readInteger();
                    if (Objects.isNull(intVal)) {
                        return null;
                    }
                    return String.valueOf(LocalDate.ofEpochDay(intVal));
                }
            case INT:
            case UINT4:
                return  String.valueOf(fieldReader.readInteger());
            case UINT8:
            case BIGINT:
                return String.valueOf(fieldReader.readLong());
            case DECIMAL:
                return String.valueOf(fieldReader.readBigDecimal());
            case FLOAT4:
                return String.valueOf(fieldReader.readFloat());
            case FLOAT8:
                return String.valueOf(fieldReader.readDouble());
            case VARCHAR: {
                    Text text = fieldReader.readText();
                    if (Objects.isNull(text)) {
                        return null;
                    }
                    return String.valueOf(text.toString());
            }
            case VARBINARY:
                return String.valueOf(fieldReader.readByteArray());
            case BIT:
                return String.valueOf(fieldReader.readBoolean());
            default:
                throw new IllegalArgumentException("Unsupported type " + minorType);
        }
    }
}
