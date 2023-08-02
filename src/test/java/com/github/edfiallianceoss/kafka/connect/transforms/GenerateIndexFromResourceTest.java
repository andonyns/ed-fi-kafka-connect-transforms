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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract class GenerateIndexFromResourceTest {

    private static final String FIELD = "test_field";
    private static final String NEW_TOPIC = "new_topic";

    @Test
    void nullSchema() {
        final SinkRecord originalRecord = record(null);
        assertThatThrownBy(() -> transformation(FIELD).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null if field name is specified: " + originalRecord);
    }

    @Test
    void noFieldName_UnsupportedValueType() {
        final SinkRecord originalRecord = record(new HashMap<String, Object>());
        assertThatThrownBy(() -> transformation(null).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage("value must specify one or more field names comma separated.");
    }

    @Test
    void fieldName_NonMap() {
        final SinkRecord originalRecord = record("some");
        assertThatThrownBy(() -> transformation(FIELD).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " type must be Map if field name is specified: " + originalRecord);
    }

    @Test
    void fieldName_NullStruct() {
        final SinkRecord originalRecord = record(null);
        assertThatThrownBy(() -> transformation(FIELD).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null if field name is specified: " + originalRecord);
    }

    @Test
    void fieldName_UnsupportedSchemaTypeInField() {
        final var field = Map.of(FIELD, Map.of());
        final SinkRecord originalRecord = record(field);
        assertThatThrownBy(() -> transformation(FIELD).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " type in " + dataPlace()
                + " " + field + " must be a comma separated string: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValue(final String value) {
        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("another", "value");
        if (!"missing".equals(value)) {
            valueMap.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(valueMap);
        assertThatThrownBy(() -> transformation(FIELD).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " in " + dataPlace() + " can't be null or empty: " + originalRecord);
    }

    @Test
    void fieldName_NormalStringValue() {
        final SinkRecord originalRecord;
        final var value = Map.of(FIELD, NEW_TOPIC);
        originalRecord = record(value);
        final SinkRecord result = transformation(FIELD).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, NEW_TOPIC));
    }

    private GenerateIndexFromResource<SinkRecord> transformation(final String fieldName) {

        final Map<String, String> props = new HashMap<>();
        if (fieldName != null) {
            props.put("field.name", fieldName);
        }
        final GenerateIndexFromResource<SinkRecord> transform = createTransformationObject();
        transform.configure(props);
        return transform;
    }

    protected abstract String dataPlace();

    protected abstract GenerateIndexFromResource<SinkRecord> createTransformationObject();

    protected abstract SinkRecord record(final Object data);

    protected SinkRecord record(final Schema keySchema,
                                final Object key,
                                final Schema valueSchema,
                                final Object value) {
        return new SinkRecord("original_topic", 0,
            keySchema, key,
            valueSchema, value,
            123L,
            456L, TimestampType.CREATE_TIME);
    }

    private SinkRecord setNewTopic(final SinkRecord record, final String newTopic) {
        return record.newRecord(newTopic,
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            record.value(),
            record.timestamp(),
            record.headers()
        );
    }
}
