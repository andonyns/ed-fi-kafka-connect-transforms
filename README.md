# Ed-Fi Transformations for Apache Kafka® Connect

[Single Message Transformations
(SMTs)](https://kafka.apache.org/documentation/#connect_transforms) for Apache
Kafka Connect.

## Transformations

See [the Kafka
documentation](https://kafka.apache.org/documentation/#connect_transforms) for
more details about configuring transformations on how to install transforms.

### `GenerateIndexFromResource`

This transformation builds an index based on a group of values contained in the
body of the result, separated by $.

- `com.github.edfiallianceoss.kafka.connect.transforms.GenerateIndexFromResource$Value`
  - works on values.

The transformation defines the following configurations:

- `field.name` - Comma separated list of fields to be included into building the
  Index. This fields will be separated by $ and will add `descriptor` at the end
  if resource is marked as such.

Here is an example of this transformation configuration:

```properties
transforms=GenerateIndexFromResource
transforms.GenerateIndexFromResource.type=com.github.edfiallianceoss.kafka.connect.transforms.GenerateIndexFromResource$Value
transforms.GenerateIndexFromResource.field.name=projectName,resourceVersion,resourceName
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
