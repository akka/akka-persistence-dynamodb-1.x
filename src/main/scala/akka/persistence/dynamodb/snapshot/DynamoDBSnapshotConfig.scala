/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.snapshot

import akka.persistence.dynamodb.{
  ClientConfig,
  DynamoDBClientConfig,
  DynamoDBConfig,
  DynamoDBTTLConfig,
  DynamoDBTTLConfigReader
}
import com.typesafe.config.Config

class DynamoDBSnapshotConfig(c: Config) extends DynamoDBConfig {
  val Table       = c.getString("snapshot-table")
  val JournalName = c.getString("journal-name")
  val AwsKey      = c.getString("aws-access-key-id")
  val AwsSecret   = c.getString("aws-secret-access-key")
  val Endpoint    = c.getString("endpoint")

  val MaxBatchGet   = c.getInt("aws-api-limits.max-batch-get")
  val MaxBatchWrite = c.getInt("aws-api-limits.max-batch-write")
  val MaxItemSize   = c.getInt("aws-api-limits.max-item-size")

  override val TTLConfig: Option[DynamoDBTTLConfig] = DynamoDBTTLConfigReader.readTTLConfig(c)

  override def toString: String =
    "DynamoDBJournalConfig(" +
    "SnapshotTable:" + Table +
    ",AwsKey:" + AwsKey +
    ",Endpoint:" + Endpoint +
    ",TTLConfig:" + TTLConfig.getOrElse("<undefined>") + ")"

  override val client: ClientConfig = new DynamoDBClientConfig(c)

  override val ClientDispatcher = c.getString("client-dispatcher")
  override val Tracing          = c.getBoolean("tracing")

}
