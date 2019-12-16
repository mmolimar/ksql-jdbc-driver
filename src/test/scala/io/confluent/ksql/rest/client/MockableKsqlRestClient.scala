package io.confluent.ksql.rest.client

import java.util.Collections.emptyMap
import java.util.Optional

import io.confluent.ksql.properties.LocalProperties

class MockableKsqlRestClient extends KsqlRestClient(
  new KsqlClient(
    emptyMap[String, String],
    Optional.empty[BasicCredentials],
    new LocalProperties(emptyMap[String, Any])
  ),
  "http://0.0.0.0",
  new LocalProperties(emptyMap[String, Any])
)
