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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract class GenerateIndexFromResourceTest {

    private static final String FIELD = "test_field";
    private static final String NEW_TOPIC = "new_topic";

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void nullSchema(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(null);
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_UnsupportedValueType(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(new HashMap<String, Object>());
        assertThatThrownBy(() -> transformation(null, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage("type in " + dataPlace() + " {} must be "
                + "[class java.lang.Byte, class java.lang.Short, class java.lang.Integer, "
                + "class java.lang.Long, class java.lang.Double, class java.lang.Float, "
                + "class java.lang.Boolean, class java.lang.String]: " + originalRecord);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_NoSkip(final String value) {
        final SinkRecord originalRecord = record(value);
        assertThatThrownBy(() -> transformation(null, false).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null or empty: " + originalRecord);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_Skip(final String value) {
        final SinkRecord originalRecord = record(value);
        final SinkRecord result = transformation(null, true).apply(originalRecord);
        assertThat(result).isEqualTo(originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_NormalInt64Value() {
        final SinkRecord originalRecord = record(123L);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "123"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_NormalBooleanValue() {
        final SinkRecord originalRecord = record(false);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "false"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_NormalStringValue() {
        final SinkRecord originalRecord = record(NEW_TOPIC);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, NEW_TOPIC));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NonMap(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record("some");
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " type must be Map if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NullStruct(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(null);
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_UnsupportedSchemaTypeInField(final boolean skipMissingOrNull) {
        final var field = Map.of(FIELD, Map.of());
        final SinkRecord originalRecord = record(field);
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " type in " + dataPlace()
                + " " + field + " must be "
                + "[class java.lang.Byte, class java.lang.Short, class java.lang.Integer, "
                + "class java.lang.Long, class java.lang.Double, class java.lang.Float, "
                + "class java.lang.Boolean, class java.lang.String]: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValue_NoSkip(final String value) {
        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("another", "value");
        if (!"missing".equals(value)) {
            valueMap.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(valueMap);
        assertThatThrownBy(() -> transformation(FIELD, false).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " in " + dataPlace() + " can't be null or empty: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValueOrMissingField_Skip(final String value) {
        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("another", "value");
        if (!"missing".equals(value)) {
            valueMap.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(valueMap);
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NormalIntValue() {
        final SinkRecord originalRecord;
        final var fieldValue = 123L;
        final var value = Map.of(FIELD, fieldValue);
        originalRecord = record(value);
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "123"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NormalBooleanValue() {
        final SinkRecord originalRecord;
        final var fieldValue = false;
        final var value = Map.of(FIELD, fieldValue);
        originalRecord = record(value);
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "false"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NormalStringValue() {
        final SinkRecord originalRecord;
        final var value = Map.of(FIELD, NEW_TOPIC);
        originalRecord = record(value);
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, NEW_TOPIC));
    }

    private GenerateIndexFromResource<SinkRecord> transformation(final String fieldName, 
                                                                 final boolean skipMissingOrNull) {
        final Map<String, String> props = new HashMap<>();
        if (fieldName != null) {
            props.put("field.name", fieldName);
        }
        props.put("skip.missing.or.null", Boolean.toString(skipMissingOrNull));
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
