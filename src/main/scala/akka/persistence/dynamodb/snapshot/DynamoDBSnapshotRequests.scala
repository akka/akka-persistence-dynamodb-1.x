/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.snapshot

import java.lang.Integer
import java.util.{ Collections, HashMap => JHMap, List => JList, Map => JMap }

import akka.persistence.dynamodb.{ DynamoDBRequests, Item }
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.serialization.Snapshot
import com.amazonaws.services.dynamodbv2.model._

import collection.JavaConverters._
import scala.concurrent.Future
import akka.persistence.dynamodb._

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

    dynamo.deleteItem(request)
      .map(toUnit)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    loadQueryResult(persistenceId, criteria).flatMap { queryResult =>
      val result = queryResult.getItems.asScala.map(item => item.get(SequenceNr).getN.toLong)
      doBatch(
        batch => s"execute batch delete $batch",
        result.map(snapshotDeleteReq(persistenceId, _)))
        .map(toUnit)
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
    dynamo.putItem(putItem(toSnapshotItem(persistenceId, sequenceNr, timestamp, snapshot)))
      .map(toUnit)
  }

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {

    loadQueryResult(persistenceId, criteria, Some(1))
      .map { result =>
        if (result.getItems.size() > 0) {
          result.getItems.asScala.headOption
            .map(youngest =>
              fromSnapshotItem(persistenceId, youngest))
        } else {
          None
        }
      }
  }

  private def loadQueryResult(persistenceId: String, criteria: SnapshotSelectionCriteria, limit: Option[Int] = None): Future[QueryResult] = {
    criteria match {
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp) if minSequenceNr == 0 && maxSequenceNr == Long.MaxValue =>
        loadByTimestamp(persistenceId, minTimestamp = minTimestamp, maxTimestamp = maxTimestamp, limit)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp) if minTimestamp == 0 && maxTimestamp == Long.MaxValue =>
        loadBySeqNr(persistenceId, minSequenceNr = minSequenceNr, maxSequenceNr = maxSequenceNr, limit)
      case _ =>
        loadByBoth(persistenceId, criteria, limit)

    }
  }

  private def loadByTimestamp(persistenceId: String, minTimestamp: Long, maxTimestamp: Long, limit: Option[Int]): Future[QueryResult] = {
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

  private def loadBySeqNr(persistenceId: String, minSequenceNr: Long, maxSequenceNr: Long, limit: Option[Int]): Future[QueryResult] = {
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

  private def loadByBoth(persistenceId: String, criteria: SnapshotSelectionCriteria, limit: Option[Int]): Future[QueryResult] = {
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

  private def toSnapshotItem(persistenceId: String, sequenceNr: Long, timestamp: Long, snapshot: Any): Item = {
    val item: Item = new JHMap

    item.put(Key, S(messagePartitionKey(persistenceId)))
    item.put(SequenceNr, N(sequenceNr))
    item.put(Timestamp, N(timestamp))
    val snp = B(serialization.serialize(Snapshot(snapshot)).get)
    item.put(Payload, snp)
    item
  }

  private def fromSnapshotItem(persistenceId: String, item: Item): SelectedSnapshot = {
    val seqNr = item.get(SequenceNr).getN.toLong
    val timestamp = item.get(Timestamp).getN.toLong
    val payloadValue = item.get(Payload).getB
    val snapshot = serialization.deserialize(payloadValue.array(), classOf[Snapshot]).get
    SelectedSnapshot(metadata = SnapshotMetadata(persistenceId, sequenceNr = seqNr, timestamp = timestamp), snapshot = snapshot.data)
  }

  private def messagePartitionKey(persistenceId: String): String =
    s"$JournalName-P-$persistenceId"

}
