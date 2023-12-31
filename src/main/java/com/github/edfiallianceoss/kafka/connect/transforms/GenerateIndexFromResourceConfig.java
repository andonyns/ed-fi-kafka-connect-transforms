/* 
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed to the Ed-Fi Alliance under one or more agreements.
 *  The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
 *  See the LICENSE and NOTICES files in the project root for more information.
 */


package com.github.edfiallianceoss.kafka.connect.transforms;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

class GenerateIndexFromResourceConfig extends AbstractConfig {
    public static final String FIELD_NAME_CONFIG = "field.name";
    private static final String FIELD_NAME_DOC =
        "The list of properties separated by comma which should be used as the topic name. ";

    GenerateIndexFromResourceConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
            .define(
                FIELD_NAME_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                FIELD_NAME_DOC);
    }

    Optional<String> fieldName() {
        final String rawFieldName = getString(FIELD_NAME_CONFIG);
        if (null == rawFieldName || "".equals(rawFieldName)) {
            return Optional.empty();
        }
        return Optional.of(rawFieldName);
    }
}
