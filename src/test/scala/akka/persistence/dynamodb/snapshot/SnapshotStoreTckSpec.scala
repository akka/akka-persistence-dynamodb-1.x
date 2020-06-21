/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.snapshot

import akka.persistence.snapshot.SnapshotStoreSpec

import com.typesafe.config.ConfigFactory

class SnapshotStoreTckSpec extends SnapshotStoreSpec(
  ConfigFactory.load()) with DynamoDBUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureSnapshotTableExists()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    client.shutdown()
  }
}
