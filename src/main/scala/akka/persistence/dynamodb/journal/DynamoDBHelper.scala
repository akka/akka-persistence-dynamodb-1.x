/**
 * Copyright (C) 2020 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.util.function.BiConsumer

import akka.actor.ActorRef
import akka.persistence.dynamodb.{ DynamoDBConfig, Item }
import akka.actor.Scheduler
import akka.event.LoggingAdapter
import akka.pattern.after
import java.util.{ concurrent => juc }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

case class LatencyReport(nanos: Long, retries: Int)
private class RetryStateHolder(var retries: Int = 10, var backoff: FiniteDuration = 1.millis)

trait DynamoDBHelper {

  implicit val ec: ExecutionContext
  val scheduler: Scheduler
  val dynamoDB: DynamoDbAsyncClient
  val log: LoggingAdapter
  val settings: DynamoDBConfig
  import settings._

  def shutdown(): Unit = dynamoDB.close()

  private var reporter: ActorRef = _
  def setReporter(ref: ActorRef): Unit = reporter = ref

  private def send[In <: DynamoDbRequest, Out](aws: In, func: In => juc.CompletableFuture[Out])(implicit d: Describe[_ >: In]): Future[Out] = {
    def name = d.desc(aws)

    def sendSingle(): Future[Out] = {
      val p = Promise[Out]
      try {
        func(aws).whenCompleteAsync(
          new BiConsumer[Out, Throwable] {
            override def accept(resp: Out, ex: Throwable): Unit = {
              if (resp != null) {
                p.trySuccess(resp)
              } else {
                // Handle the error
                ex match {
                  case e: ProvisionedThroughputExceededException =>
                    p.tryFailure(e)
                  case _ =>
                    val n = name
                    log.error(ex, "failure while executing {}", n)
                    p.tryFailure(new DynamoDBJournalFailure("failure while executing " + n, ex))
                }
              }
            }
          })
      } catch {
        case ex: Throwable =>
          log.error(ex, "failure while preparing {}", name)
          p.tryFailure(ex)
      }
      p.future
    }

    val state = new RetryStateHolder

    lazy val retry: PartialFunction[Throwable, Future[Out]] = {
      case _: ProvisionedThroughputExceededException if state.retries > 0 =>
        val backoff = state.backoff
        state.retries -= 1
        state.backoff *= 2
        after(backoff, scheduler)(sendSingle().recoverWith(retry))
      case other => Future.failed(other)
    }

    if (Tracing) log.debug("{}", name)
    val start = if (reporter ne null) System.nanoTime else 0L

    // backoff retries when sending too fast
    val f = sendSingle().recoverWith(retry)

    if (reporter ne null) f.onComplete(_ => reporter ! LatencyReport(System.nanoTime - start, 10 - state.retries))

    f
  }

  trait Describe[T] {
    def desc(t: T): String
    protected def formatKey(i: Item): String = {
      val key = i.get(Key) match { case null => "<none>" case x => x.s }
      val sort = i.get(Sort) match { case null => "<none>" case x => x.n }
      s"[$Key=$key,$Sort=$sort]"
    }
  }

  object Describe {
    implicit object GenericDescribe extends Describe[DynamoDbRequest] {
      def desc(aws: DynamoDbRequest): String = aws.getClass.getSimpleName
    }
  }

  implicit object DescribeDescribe extends Describe[DescribeTableRequest] {
    def desc(aws: DescribeTableRequest): String = s"DescribeTableRequest(${aws.tableName()})"
  }

  implicit object QueryDescribe extends Describe[QueryRequest] {
    def desc(aws: QueryRequest): String = s"QueryRequest(${aws.tableName},${aws.expressionAttributeValues})"
  }

  implicit object PutItemDescribe extends Describe[PutItemRequest] {
    def desc(aws: PutItemRequest): String = s"PutItemRequest(${aws.tableName},${formatKey(aws.item)})"
  }

  implicit object DeleteDescribe extends Describe[DeleteItemRequest] {
    def desc(aws: DeleteItemRequest): String = s"DeleteItemRequest(${aws.tableName},${formatKey(aws.key)})"
  }

  implicit object BatchGetItemDescribe extends Describe[BatchGetItemRequest] {
    def desc(aws: BatchGetItemRequest): String = {
      val entry = aws.requestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys = entry.getValue.keys().asScala.map(formatKey)
      s"BatchGetItemRequest($table, ${keys.mkString("(", ",", ")")})"
    }
  }

  implicit object BatchWriteItemDescribe extends Describe[BatchWriteItemRequest] {
    def desc(aws: BatchWriteItemRequest): String = {
      val entry = aws.requestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys = entry.getValue.asScala.map { write =>
        write.deleteRequest() match {
          case null => "put" + formatKey(write.putRequest.item)
          case del  => "del" + formatKey(del.key)
        }
      }
      s"BatchWriteItemRequest($table, ${keys.mkString("(", ",", ")")})"
    }
  }

  def listTables(aws: ListTablesRequest): Future[ListTablesResponse] =
    send[ListTablesRequest, ListTablesResponse](aws, dynamoDB.listTables)

  def describeTable(aws: DescribeTableRequest): Future[DescribeTableResponse] =
    send[DescribeTableRequest, DescribeTableResponse](aws, dynamoDB.describeTable)

  def createTable(aws: CreateTableRequest): Future[CreateTableResponse] =
    send[CreateTableRequest, CreateTableResponse](aws, dynamoDB.createTable)

  def updateTable(aws: UpdateTableRequest): Future[UpdateTableResponse] =
    send[UpdateTableRequest, UpdateTableResponse](aws, dynamoDB.updateTable)

  def deleteTable(aws: DeleteTableRequest): Future[DeleteTableResponse] =
    send[DeleteTableRequest, DeleteTableResponse](aws, dynamoDB.deleteTable)

  def query(aws: QueryRequest): Future[QueryResponse] =
    send[QueryRequest, QueryResponse](aws, dynamoDB.query)

  def scan(aws: ScanRequest): Future[ScanResponse] =
    send[ScanRequest, ScanResponse](aws, dynamoDB.scan)

  def putItem(aws: PutItemRequest): Future[PutItemResponse] =
    send[PutItemRequest, PutItemResponse](aws, dynamoDB.putItem)

  def getItem(aws: GetItemRequest): Future[GetItemResponse] =
    send[GetItemRequest, GetItemResponse](aws, dynamoDB.getItem)

  def updateItem(aws: UpdateItemRequest): Future[UpdateItemResponse] =
    send[UpdateItemRequest, UpdateItemResponse](aws, dynamoDB.updateItem)

  def deleteItem(aws: DeleteItemRequest): Future[DeleteItemResponse] =
    send[DeleteItemRequest, DeleteItemResponse](aws, dynamoDB.deleteItem)

  def batchWriteItem(aws: BatchWriteItemRequest): Future[BatchWriteItemResponse] =
    send[BatchWriteItemRequest, BatchWriteItemResponse](aws, dynamoDB.batchWriteItem)

  def batchGetItem(aws: BatchGetItemRequest): Future[BatchGetItemResponse] =
    send[BatchGetItemRequest, BatchGetItemResponse](aws, dynamoDB.batchGetItem)

}
