/*
 * Copyright 2019 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.edfiallianceoss.kafka.connect.transforms;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class GenerateIndexFromResource<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final List<Class<?>> SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM = Arrays.asList(
        Byte.class,
        Short.class,
        Integer.class,
        Long.class,
        Double.class,
        Float.class,
        Boolean.class,
        String.class
    );

    private GenerateIndexFromResourceConfig config;

    @Override
    public ConfigDef config() {
        return GenerateIndexFromResourceConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> settings) {
        this.config = new GenerateIndexFromResourceConfig(settings);
    }

    @Override
    public R apply(final R record) {
        final SchemaAndValue schemaAndValue = getSchemaAndValue(record);

        final Optional<String> newTopic;

        if (!config.fieldName().isPresent()) {
            throw new DataException(dataPlace() + " must specify one or more field names comma separated.");
        }

        List<String> fieldList = Stream.of(config.fieldName().get().split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        final String separator = "$";
        StringBuilder topicResult = new StringBuilder();

        fieldList.forEach(field -> {
            topicResult.append(topicNameFromNamedField(record.toString(), schemaAndValue.value(), field).get() + separator);
        });

        topicResult.replace(topicResult.length() - 1, topicResult.length(), "");

        if(record.toString().contains("isDescriptor=true")) {
            topicResult.append("descriptor");
        }

        newTopic = Optional.of(topicResult.toString());

        if (newTopic.isPresent()) {
            return record.newRecord(
                newTopic.get(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()
            );
        } else {
            return record;
        }
    }

    protected abstract String dataPlace();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    private Optional<String> topicNameFromNamedField(final String recordStr,
                                                               final Object value,
                                                               final String fieldName) {
        if (value == null) {
            throw new DataException(dataPlace() + " can't be null if field name is specified: " + recordStr);
        }

        if (!(value instanceof Map)) {
            throw new DataException(dataPlace() + " type must be Map if field name is specified: " + recordStr);
        }

        @SuppressWarnings("unchecked") final Map<String, Object> valueMap = (Map<String, Object>) value;

        final Optional<String> result = Optional.ofNullable(valueMap.get(fieldName))
            .map(field -> {
                if (!SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM.contains(field.getClass())) {
                    throw new DataException(fieldName + " type in " + dataPlace()
                        + " " + value
                        + " must be " + SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM
                        + ": " + recordStr);
                }
                return field;
            })
            .map(Object::toString);

        if (result.isPresent() && !result.get().isBlank()) {
            return result;
        } else {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " can't be null or empty: " + recordStr);
            }
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends GenerateIndexFromResource<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        @Override
        protected String dataPlace() {
            return "key";
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends GenerateIndexFromResource<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        protected String dataPlace() {
            return "value";
        }
    }

    @Override
    public void close() {
    }
}
