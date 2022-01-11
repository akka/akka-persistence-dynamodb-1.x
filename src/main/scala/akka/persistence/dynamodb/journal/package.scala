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
import scala.util.{ Failure, Success, Try }
import java.util.concurrent.{ LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit }
import scala.collection.generic.CanBuildFrom
import java.util.concurrent.Executors
import java.util.Collections
import java.nio.ByteBuffer

package object journal {

  // field names
  val Key        = "par"
  val Sort       = "num"
  val Payload    = "pay"
  val SequenceNr = "seq"
  val AtomIndex  = "idx"
  val AtomEnd    = "cnt"

  /* PersistenceRepr fields
   sequence_nr and persistence_id extracted from the key
   */
  val PersistentId = "persistence_id"
  val WriterUuid   = "writer_uuid"

  val Manifest = "manifest"

  val Event              = "event"
  val SerializerId       = "ev_ser_id"
  val SerializerManifest = "ev_ser_manifest"

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
