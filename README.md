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

## Legal Information

Copyright (c) 2023 Ed-Fi Alliance, LLC and contributors.

Licensed under the [Apache License, Version 2.0](https://github.com/Ed-Fi-Exchange-OSS/Meadowlark/blob/main/LICENSE) (the "License").

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

See [NOTICES](https://github.com/Ed-Fi-Exchange-OSS/Meadowlark/blob/main/NOTICES.md) for additional copyright and license notifications.
