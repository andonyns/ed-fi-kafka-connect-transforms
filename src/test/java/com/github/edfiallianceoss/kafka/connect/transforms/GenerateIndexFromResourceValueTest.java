/* 
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed to the Ed-Fi Alliance under one or more agreements.
 *  The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
 *  See the LICENSE and NOTICES files in the project root for more information.
 */

package com.github.edfiallianceoss.kafka.connect.transforms;

import org.apache.kafka.connect.sink.SinkRecord;

public class GenerateIndexFromResourceValueTest extends GenerateIndexFromResourceTest {

    @Override
    protected GenerateIndexFromResource<SinkRecord> createTransformationObject() {
        return new GenerateIndexFromResource.Value<>();
    }

    @Override
    protected SinkRecord record(final Object data) {
        return record(null, null, null, data);
    }
}
