/* 
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed to the Ed-Fi Alliance under one or more agreements.
 *  The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
 *  See the LICENSE and NOTICES files in the project root for more information.
 */


package com.github.edfiallianceoss.kafka.connect.transforms;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GenerateIndexFromResourceConfigTest {

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "");
        final GenerateIndexFromResourceConfig config = new GenerateIndexFromResourceConfig(props);
        assertThat(config.fieldName()).isNotPresent();
    }

    @Test
    void definedFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "test");
        final GenerateIndexFromResourceConfig config = new GenerateIndexFromResourceConfig(props);
        assertThat(config.fieldName()).hasValue("test");
    }
}
