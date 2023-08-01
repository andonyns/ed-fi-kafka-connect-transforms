# Ed-Fi Transformations for Apache Kafka® Connect

[Single Message Transformations
(SMTs)](https://kafka.apache.org/documentation/#connect_transforms) for Apache
Kafka Connect.

## Transformations

See [the Kafka
documentation](https://kafka.apache.org/documentation/#connect_transforms) for
more details about configuring transformations on how to install transforms.

### `GenerateIndexFromResource`

This transformation builds an index based on a group of values contained in the body of the result, separated by $.

- `com.github.edfiallianceoss.kafka.connect.transforms.GenerateIndexFromResource$Value` -
  works on values.

The transformation defines the following configurations:

- `field.name` - The name of the field which should be used as the topic name.
  If `null` or empty, the entire key or value is used (and assumed to be a
  string). By default is `null`.
- `skip.missing.or.null` - In case the source of the new topic name is `null` or
  missing, should a record be silently passed without transformation. By
  default, is `false`.

Here is an example of this transformation configuration:

```properties
transforms=GenerateIndexFromResource
transforms.GenerateIndexFromResource.type=com.github.edfiallianceoss.kafka.connect.transforms.GenerateIndexFromResource$Value
transforms.GenerateIndexFromResource.field.name=inner_field_name
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

The transformation `GenerateIndexFromResource` is based on the [ExtractTopic
transformation](https://github.com/Aiven-Open/transforms-for-apache-kafka-connect/blob/master/src/main/java/io/aiven/kafka/connect/transforms/ExtractTopic.java)
by Aiven Open. Licensed under Apache License, Version 2.0.

## Trademarks

Apache Kafka and Apache Kafka Connect are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries.
