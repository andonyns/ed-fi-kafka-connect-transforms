/* 
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed to the Ed-Fi Alliance under one or more agreements.
 *  The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
 *  See the LICENSE and NOTICES files in the project root for more information.
 */


package com.github.edfiallianceoss.kafka.connect.transforms;

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

public class GenerateIndexFromResource<R extends ConnectRecord<R>> implements Transformation<R> {

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
            throw new DataException("value must specify one or more field names comma separated.");
        }

        final List<String> fieldList = Stream.of(config.fieldName().get().split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        final StringBuilder topicResult = new StringBuilder();
        final String separator = "$";

        fieldList.forEach(field -> {
            topicResult.append(
                topicNameFromNamedField(record.toString(), 
                                        schemaAndValue.value(), 
                                        field).get() + separator);
        });

        topicResult.replace(topicResult.length() - 1, topicResult.length(), "");

        if (record.toString().contains("isDescriptor=true")) {
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

    protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
    }

    private Optional<String> topicNameFromNamedField(final String recordStr,
                                                               final Object value,
                                                               final String fieldName) {
        if (value == null) {
            throw new DataException("value can't be null: " + recordStr);
        }

        if (!(value instanceof Map)) {
            throw new DataException("value type must be an object: " + recordStr);
        }

        @SuppressWarnings("unchecked") final Map<String, Object> valueMap = (Map<String, Object>) value;

        final Optional<String> result = Optional.ofNullable(valueMap.get(fieldName))
            .map(field -> {
                if (!field.getClass().equals(String.class)) {
                    throw new DataException(fieldName + " type in value "
                        + value
                        + " must be a comma separated string: "
                        + recordStr);
                }
                return field;
            })
            .map(Object::toString);

        if (result.isPresent() && !result.get().isBlank()) {
            return result;
        } else {
            throw new DataException(fieldName + " in value can't be null or empty: " + recordStr);
        }
    }

    @Override
    public void close() {
    }
}
