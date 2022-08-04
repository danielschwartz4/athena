/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.List;
import java.util.Map;

/**
 * This class has interfaces used for document field-values extraction after they are retrieved from an Elasticsearch
 * instance. This includes field extractors and field writer factories using the field extractor framework.
 */
class EFSExtractorTypeUtils
{
    private static final Logger logger = LoggerFactory.getLogger(EFSExtractorTypeUtils.class);

    /**
     * Create the appropriate field extractor used for extracting field values from a Document based on the field type.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    protected Extractor makeExtractor(Field field, int index)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        switch (fieldType) {
            case VARCHAR:
                return makeVarCharExtractor(field, index);
            case BIGINT:
                return makeBigIntExtractor(field, index);
            case INT:
                return this.makeIntExtractor(field, index);
            case SMALLINT:
                return makeSmallIntExtractor(field, index);
            case TINYINT:
                return makeTinyIntExtractor(field, index);
            case FLOAT8:
                return makeFloat8Extractor(field, index);
            case FLOAT4:
                return makeFloat4Extractor(field, index);
            case DATEMILLI:
                return makeDateMilliExtractor(field);
            case BIT:
                return makeBitExtractor(field, index);
            default:
                return null;
        }
    }

    /**
     * Create a VARCHAR field extractor to extract a string value from a Document. The Document value can be returned
     * as a String or a List. For the latter, extract the first element only.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeVarCharExtractor(Field field, int index)
    {
        return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
        {
            String fieldValue = String.valueOf(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof String) {
                dst.value = fieldValue;
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create a BIGINT field extractor to extract a long value from a Document. The Document value can be returned
     * as a numeric value, a String, or a List. For the latter, extract the first element only. Special logic
     * is employed to parse out a scaled_float which is a float represented as a long (scaled by a double value).
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeBigIntExtractor(Field field, int index)
    {
        return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
        {
            Integer fieldValue = Integer.parseInt(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof Number) {
                dst.value = ((Number) fieldValue).intValue();
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create an INT field extractor to extract an integer value from a Document. The Document value can be returned
     * as a numeric value, a String, or a List. For the latter, extract the first element only.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeIntExtractor(Field field, int index)
    {
        return (IntExtractor) (Object context, NullableIntHolder dst) ->
        {
//            Object fieldValue = ((Map) context).get(field.getName());
            Integer fieldValue = Integer.parseInt(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof Number) {
                dst.value = ((Number) fieldValue).intValue();
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create an SMALLINT field extractor to extract a short value from a Document. The Document value can be returned
     * as a numeric value, a String, or a List. For the latter, extract the first element only.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeSmallIntExtractor(Field field, int index)
    {
        return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
        {
            Short fieldValue = Short.valueOf(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof Number) {
                dst.value = ((Number) fieldValue).shortValue();
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create an TINYINT field extractor to extract a byte value from a Document. The Document value can be returned
     * as a numeric value, a String, or a List. For the latter, extract the first element only.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeTinyIntExtractor(Field field, int index)
    {
        return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
        {
            Short fieldValue = Short.valueOf(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof Number) {
                dst.value = ((Number) fieldValue).byteValue();
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create an FLOAT8 field extractor to extract a double value from a Document. The Document value can be returned
     * as a numeric value, a String, or a List. For the latter, extract the first element only.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeFloat8Extractor(Field field, int index)
    {
        return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
        {
            Float fieldValue = Float.parseFloat(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof Number) {
                dst.value = ((Number) fieldValue).doubleValue();
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create an FLOAT4 field extractor to extract a float value from a Document. The Document value can be returned
     * as a numeric value, a String, or a List. For the latter, extract the first element only.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeFloat4Extractor(Field field, int index)
    {
        return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
        {
            Float fieldValue = Float.parseFloat(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof Number) {
                dst.value = ((Number) fieldValue).floatValue();
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create an DATEMILLI field extractor to extract a date value from a Document. The Document value can be returned
     * as a numeric value, a String, or a List. For the latter, extract the first element only.
     * For dates extracted as a string, the ISO_ZONED_DATE_TIME format will be attempted first, followed by the
     * ISO_LOCAL_DATE_TIME format if the previous one fails. Examples of formats that will work:
     * 1) "2020-05-18T10:15:30.123456789"
     * 2) "2020-05-15T06:50:01.123Z"
     * 3) "2020-05-15T06:49:30.123-05:00".
     * Numeric dates values should be a long numeric value representing epoch milliseconds (e.g. 1589525370001)
     * Nanoseconds will be rounded to the nearest millisecond.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeDateMilliExtractor(Field field)
    {
        return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
        {
            Object fieldValue = ((Map) context).get(field.getName());
            dst.isSet = 1;
            if (fieldValue instanceof String) {
                try {
                    long epochSeconds;
                    double nanoSeconds;
                    try {
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) fieldValue,
                                DateTimeFormatter.ISO_ZONED_DATE_TIME.withResolverStyle(ResolverStyle.SMART));
                        epochSeconds = zonedDateTime.toEpochSecond();
                        nanoSeconds = zonedDateTime.getNano();
                    }
                    catch (DateTimeParseException error) {
                        LocalDateTime localDateTime = LocalDateTime.parse((String) fieldValue,
                                DateTimeFormatter.ISO_LOCAL_DATE_TIME.withResolverStyle(ResolverStyle.SMART));
                        epochSeconds = localDateTime.toEpochSecond(ZoneOffset.UTC);
                        nanoSeconds = localDateTime.getNano();
                    }
                    dst.value = epochSeconds * 1000 + Math.round(nanoSeconds / 1000000);
                }
                catch (DateTimeParseException error) {
                    logger.warn("Error parsing localDateTime: {}.", error.getMessage());
                    dst.isSet = 0;
                }
            }
            else if (fieldValue instanceof Number) {
                dst.value = ((Number) fieldValue).longValue();
            }
            else if (fieldValue instanceof List) {
                Object value = ((List) fieldValue).get(0);
                if (value instanceof String) {
                    try {
                        long epochSeconds;
                        double nanoSeconds;
                        try {
                            ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) value,
                                    DateTimeFormatter.ISO_ZONED_DATE_TIME.withZone(ZoneId.of("UTC"))
                                            .withResolverStyle(ResolverStyle.SMART));
                            epochSeconds = zonedDateTime.toEpochSecond();
                            nanoSeconds = zonedDateTime.getNano();
                        }
                        catch (DateTimeParseException error) {
                            LocalDateTime localDateTime = LocalDateTime.parse((String) value,
                                    DateTimeFormatter.ISO_LOCAL_DATE_TIME
                                            .withResolverStyle(ResolverStyle.SMART));
                            epochSeconds = localDateTime.toEpochSecond(ZoneOffset.UTC);
                            nanoSeconds = localDateTime.getNano();
                        }
                        dst.value = epochSeconds * 1000 + Math.round(nanoSeconds / 1000000);
                    }
                    catch (DateTimeParseException error) {
                        logger.warn("Error parsing localDateTime: {}.", error.getMessage());
                        dst.isSet = 0;
                    }
                }
                else if (value instanceof Number) {
                    dst.value = ((Number) value).longValue();
                }
                else {
                    dst.isSet = 0;
                }
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Create an BIT field extractor to extract a boolean value from a Document. The Document value can be returned
     * as a boolean value, a String, or a List. For the latter, extract the first element only.
     * @param field is used to determine which extractor to generate based on the field type.
     * @return a field extractor.
     */
    private Extractor makeBitExtractor(Field field, int index)
    {
        return (BitExtractor) (Object context, NullableBitHolder dst) ->
        {
            Boolean fieldValue = Boolean.parseBoolean(((String[]) context)[index]);
            dst.isSet = 1;
            if (fieldValue instanceof Boolean) {
                boolean booleanValue = (Boolean) fieldValue;
                dst.value = booleanValue ? 1 : 0;
            }
            else {
                dst.isSet = 0;
            }
        };
    }

    /**
     * Since GeneratedRowWriter doesn't yet support complex types (STRUCT, LIST) we use this to create our own
     * FieldWriters via a custom FieldWriterFactory.
     * @param field is used to determine which factory to generate based on the field type.
     * @return a field writing factory.
     * @throws IllegalArgumentException
     */
//    protected FieldWriterFactory makeFactory(Field field)
//            throws RuntimeException
//    {
//        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
////        switch (fieldType) {
////            case LIST:
////                //Field child = field.getChildren().get(0);
////                //Types.MinorType childType = Types.getMinorTypeForArrowType(child.getType());
////                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
////                        (FieldWriter) (Object context, int rowNum) ->
////                        {
////                            Object fieldValue = ((Map) context).get(field.getName());
////                            BlockUtils.setComplexValue(vector, rowNum, fieldResolver,
////                                    fieldResolver.coerceListField(field, fieldValue));
////                            return true;
////                        };
////            case STRUCT:
////                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
////                        (FieldWriter) (Object context, int rowNum) ->
////                        {
////                            Object fieldValue = ((Map) context).get(field.getName());
////                            BlockUtils.setComplexValue(vector, rowNum, fieldResolver, fieldValue);
////                            return true;
////                        };
////            default:
////                throw new RuntimeException(fieldType + " is not supported");
////        }
//    }
}
