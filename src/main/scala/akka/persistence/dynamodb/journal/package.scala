/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import akka.actor.{ ActorSystem, Scheduler }
import akka.event.{ Logging, LoggingAdapter }
import java.util.{ Map => JMap }
import scala.concurrent._
import scala.util.{ Try, Success, Failure }
import java.util.concurrent.{ ThreadPoolExecutor, LinkedBlockingQueue, TimeUnit }
import scala.collection.generic.CanBuildFrom
import java.util.concurrent.Executors
import java.util.Collections
import java.nio.ByteBuffer

package object journal {

  // field names
  val Key = "par"
  val Sort = "num"
  val Payload = "pay"
  val SequenceNr = "seq"
  val AtomIndex = "idx"
  val AtomEnd = "cnt"

  val KeyPayloadOverhead = 26 // including fixed parts of partition key and 36 bytes fudge factor

  import collection.JavaConverters._

  val schema = new CreateTableRequest()
    .withKeySchema(
      new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(Sort).withKeyType(KeyType.RANGE))
    .withAttributeDefinitions(
      new AttributeDefinition().withAttributeName(Key).withAttributeType("S"),
      new AttributeDefinition().withAttributeName(Sort).withAttributeType("N"))

}
