/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

import java.util.Collections
import java.util.{ Collections, HashMap => JHMap, List => JList, Map => JMap }

import akka.Done
import akka.actor.{ Actor, ActorLogging }
import akka.persistence.dynamodb.journal.DynamoDBHelper

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.concurrent.duration._
import akka.pattern.after
import software.amazon.awssdk.services.dynamodb.model._

private[dynamodb] trait DynamoDBRequests {
  this: ActorLogging with Actor =>

  val settings: DynamoDBConfig
  def dynamo: DynamoDBHelper

  import context.dispatcher
  import settings._

  def putItem(item: Item): PutItemRequest = PutItemRequest.builder().tableName(Table).item(item).build()

  def batchWriteReq(writes: Seq[WriteRequest]): BatchWriteItemRequest =
    batchWriteReq(Collections.singletonMap(Table, writes.asJava))

  def batchWriteReq(items: JMap[String, JList[WriteRequest]]): BatchWriteItemRequest =
    BatchWriteItemRequest.builder()
      .requestItems(items)
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()

  /*
   * Request execution helpers.
   */

  /**
   * Execute the given WriteRequests in batches of MaxBatchWrite, ignoring and
   * logging all errors. The returned Future never fails.
   */
  def doBatch(desc: Seq[WriteRequest] => String, writes: Seq[WriteRequest]): Future[Done] =
    Future.sequence {
      writes
        .grouped(MaxBatchWrite)
        .map { batch =>
          dynamo.batchWriteItem(batchWriteReq(batch))
            .flatMap(sendUnprocessedItems(_))
            .recover {
              case NonFatal(ex) => log.error(ex, "cannot " + desc(batch))
            }
        }
    }.map(_ => Done)

  /**
   * Sends the unprocessed batch write items, and sets the back-off.
   * if no more retries remain (number of back-off retries exhausted), we throw a Runtime exception
   *
   * Note: the DynamoDB client supports automatic retries, however a batch will not fail if some of the items in the
   * batch fail; that is why we need our own back-off mechanism here.  If we exhaust OUR retry logic on top of
   * the retries from the client, then we are hosed and cannot continue; that is why we have a RuntimeException here
   */
  private def sendUnprocessedItems(
    result:           BatchWriteItemResponse,
    retriesRemaining: Int                    = 10,
    backoff:          FiniteDuration         = 1.millis): Future[BatchWriteItemResponse] = {
    val unprocessed: Int = result.unprocessedItems.get(Table) match {
      case null  => 0
      case items => items.size
    }
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      throw new RuntimeException(s"unable to batch write ${result.unprocessedItems.get(Table)} after 10 tries")
    } else {
      val rest = batchWriteReq(result.unprocessedItems)
      after(backoff, context.system.scheduler)(dynamo.batchWriteItem(rest).flatMap(r => sendUnprocessedItems(r, retriesRemaining - 1, backoff * 2)))
    }
  }

}
