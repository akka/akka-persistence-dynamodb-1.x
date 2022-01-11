/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.snapshot

import akka.persistence.dynamodb.IntegSpec

import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.SnapshotProtocol._
import akka.persistence._
import akka.persistence.scalatest.OptionalTests
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.immutable.Seq

class SnapshotStoreTckSpec extends SnapshotStoreSpec(ConfigFactory.load()) with DynamoDBUtils with IntegSpec {
  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureSnapshotTableExists()
  }

  override def afterAll(): Unit = {
    client.shutdown()
    super.afterAll()
  }
}
