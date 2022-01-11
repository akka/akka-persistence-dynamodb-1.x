/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.snapshot

import java.util.{ HashMap => JHMap }
import akka.actor.ExtendedActorSystem
import akka.persistence.dynamodb.{ DynamoDBRequests, Item }
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.serialization.Snapshot
import com.amazonaws.services.dynamodbv2.model._

import collection.JavaConverters._
import scala.concurrent.Future
import akka.persistence.dynamodb._
import akka.serialization.{ AsyncSerializer, Serialization, Serializers }

import java.time.OffsetDateTime

trait DynamoDBSnapshotRequests extends DynamoDBRequests {
  this: DynamoDBSnapshotStore =>

  import settings._
  import context.dispatcher

  val toUnit: Any => Unit = _ => ()

  def delete(metadata: SnapshotMetadata): Future[Unit] = {
    val request = new DeleteItemRequest()
      .withTableName(Table)
      .addKeyEntry(Key, S(messagePartitionKey(metadata.persistenceId)))
      .addKeyEntry(SequenceNr, N(metadata.sequenceNr))

    dynamo.deleteItem(request).map(toUnit)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    loadQueryResult(persistenceId, criteria).flatMap { queryResult =>
      val result = queryResult.getItems.asScala.toSeq.map(item => item.get(SequenceNr).getN.toLong)
      doBatch(batch => s"execute batch delete $batch", result.map(snapshotDeleteReq(persistenceId, _))).map(toUnit)
    }
  }

  private def snapshotDeleteReq(persistenceId: String, sequenceNr: Long): WriteRequest = {
    new WriteRequest().withDeleteRequest(new DeleteRequest().withKey {
      val item: Item = new JHMap
      item.put(Key, S(messagePartitionKey(persistenceId)))
      item.put(SequenceNr, N(sequenceNr))
      item
    })
  }

  def save(persistenceId: String, sequenceNr: Long, timestamp: Long, snapshot: Any): Future[Unit] = {
    toSnapshotItem(persistenceId, sequenceNr, timestamp, snapshot).flatMap { snapshotItem =>
      dynamo.putItem(putItem(snapshotItem)).map(toUnit)
    }
  }

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {

    loadQueryResult(persistenceId, criteria, Some(1)).flatMap { result =>
      result.getItems.asScala.headOption match {
        case Some(youngest) => fromSnapshotItem(persistenceId, youngest).map(Some(_))
        case None           => Future.successful(None)
      }
    }
  }

  private def loadQueryResult(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria,
      limit: Option[Int] = None): Future[QueryResult] = {
    criteria match {
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp)
          if minSequenceNr == 0 && maxSequenceNr == Long.MaxValue =>
        loadByTimestamp(persistenceId, minTimestamp = minTimestamp, maxTimestamp = maxTimestamp, limit)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp)
          if minTimestamp == 0 && maxTimestamp == Long.MaxValue =>
        loadBySeqNr(persistenceId, minSequenceNr = minSequenceNr, maxSequenceNr = maxSequenceNr, limit)
      case _ =>
        loadByBoth(persistenceId, criteria, limit)

    }
  }

  private def loadByTimestamp(
      persistenceId: String,
      minTimestamp: Long,
      maxTimestamp: Long,
      limit: Option[Int]): Future[QueryResult] = {
    val request = new QueryRequest()
      .withTableName(Table)
      .withIndexName(TimestampIndex)
      .withKeyConditionExpression(s" $Key = :partitionKeyVal AND $Timestamp BETWEEN :tsMinVal AND :tsMaxVal ")
      .addExpressionAttributeValuesEntry(":partitionKeyVal", S(messagePartitionKey(persistenceId)))
      .addExpressionAttributeValuesEntry(":tsMinVal", N(minTimestamp))
      .addExpressionAttributeValuesEntry(":tsMaxVal", N(maxTimestamp))
      .withScanIndexForward(false)
      .withConsistentRead(true)
    limit.foreach(request.setLimit(_))

    dynamo.query(request)
  }

  private def loadBySeqNr(
      persistenceId: String,
      minSequenceNr: Long,
      maxSequenceNr: Long,
      limit: Option[Int]): Future[QueryResult] = {
    val request = new QueryRequest()
      .withTableName(Table)
      .withKeyConditionExpression(s" $Key = :partitionKeyVal AND $SequenceNr BETWEEN :seqMinVal AND :seqMaxVal")
      .addExpressionAttributeValuesEntry(":partitionKeyVal", S(messagePartitionKey(persistenceId)))
      .addExpressionAttributeValuesEntry(":seqMinVal", N(minSequenceNr))
      .addExpressionAttributeValuesEntry(":seqMaxVal", N(maxSequenceNr))
      .withScanIndexForward(false)
      .withConsistentRead(true)
    limit.foreach(request.setLimit(_))

    dynamo.query(request)
  }

  private def loadByBoth(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria,
      limit: Option[Int]): Future[QueryResult] = {
    val request = new QueryRequest()
      .withTableName(Table)
      .withKeyConditionExpression(s" $Key = :partitionKeyVal AND $SequenceNr BETWEEN :seqMinVal AND :seqMaxVal")
      .addExpressionAttributeValuesEntry(":partitionKeyVal", S(messagePartitionKey(persistenceId)))
      .addExpressionAttributeValuesEntry(":seqMinVal", N(criteria.minSequenceNr))
      .addExpressionAttributeValuesEntry(":seqMaxVal", N(criteria.maxSequenceNr))
      .withScanIndexForward(false)
      .withFilterExpression(s"$Timestamp BETWEEN :tsMinVal AND :tsMaxVal ")
      .addExpressionAttributeValuesEntry(":tsMinVal", N(criteria.minTimestamp))
      .addExpressionAttributeValuesEntry(":tsMaxVal", N(criteria.maxTimestamp))
      .withConsistentRead(true)
    limit.foreach(request.setLimit(_))

    dynamo.query(request)
  }

  private def toSnapshotItem(persistenceId: String, sequenceNr: Long, timestamp: Long, snapshot: Any): Future[Item] = {
    val item: Item = new JHMap

    item.put(Key, S(messagePartitionKey(persistenceId)))
    item.put(SequenceNr, N(sequenceNr))
    item.put(Timestamp, N(timestamp))
    val snapshotData = snapshot.asInstanceOf[AnyRef]
    val serializer   = serialization.findSerializerFor(snapshotData)
    val manifest     = Serializers.manifestFor(serializer, snapshotData)

    val fut = serializer match {
      case asyncSer: AsyncSerializer =>
        Serialization.withTransportInformation(context.system.asInstanceOf[ExtendedActorSystem]) { () =>
          asyncSer.toBinaryAsync(snapshotData)
        }
      case _ =>
        Future {
          // Serialization.serialize adds transport info
          serialization.serialize(snapshotData).get
        }
    }

    fut.map { data =>
      item.put(PayloadData, B(data))

      TTLConfig.foreach {
        case DynamoDBTTLConfig(fieldName, ttl) =>
          val expiresAt = ttl.getItemExpiryTimeEpochSeconds(OffsetDateTime.now)
          item.put(fieldName, N(expiresAt))
      }

      if (manifest.nonEmpty) {
        item.put(SerializerManifest, S(manifest))
      }
      item.put(SerializerId, N(serializer.identifier))
      item
    }
  }

  private def fromSnapshotItem(persistenceId: String, item: Item): Future[SelectedSnapshot] = {
    val seqNr     = item.get(SequenceNr).getN.toLong
    val timestamp = item.get(Timestamp).getN.toLong

    if (item.containsKey(PayloadData)) {

      val payloadData = item.get(PayloadData).getB
      val serId       = item.get(SerializerId).getN.toInt
      val manifest    = if (item.containsKey(SerializerManifest)) item.get(SerializerManifest).getS else ""

      val serialized = serialization.serializerByIdentity(serId) match {
        case aS: AsyncSerializer =>
          Serialization.withTransportInformation(context.system.asInstanceOf[ExtendedActorSystem]) { () =>
            aS.fromBinaryAsync(payloadData.array(), manifest)
          }
        case _ =>
          Future.successful(serialization.deserialize(payloadData.array(), serId, manifest).get)
      }

      serialized.map(
        data =>
          SelectedSnapshot(
            metadata = SnapshotMetadata(persistenceId, sequenceNr = seqNr, timestamp = timestamp),
            snapshot = data))

    } else {
      val payloadValue = item.get(Payload).getB
      Future.successful(
        SelectedSnapshot(
          metadata = SnapshotMetadata(persistenceId, sequenceNr = seqNr, timestamp = timestamp),
          snapshot = serialization.deserialize(payloadValue.array(), classOf[Snapshot]).get.data))
    }
  }

  private def messagePartitionKey(persistenceId: String): String =
    s"$JournalName-P-$persistenceId"

}
