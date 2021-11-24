/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import com.amazonaws.{ AmazonServiceException, AmazonWebServiceRequest }
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import akka.actor.Scheduler
import akka.event.LoggingAdapter
import akka.pattern.after
import java.util.{ concurrent => juc }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace
import akka.actor.ActorRef
import akka.persistence.dynamodb.{ DynamoDBConfig, Item }

case class LatencyReport(nanos: Long, retries: Int)
private class RetryStateHolder(var retries: Int = 10, var backoff: FiniteDuration = 1.millis)

trait DynamoDBHelper {

  implicit val ec: ExecutionContext
  val scheduler: Scheduler
  val dynamoDB: AmazonDynamoDBAsyncClient
  val log: LoggingAdapter
  val settings: DynamoDBConfig
  import settings._

  def shutdown(): Unit = dynamoDB.shutdown()

  private var reporter: ActorRef       = _
  def setReporter(ref: ActorRef): Unit = reporter = ref

  private def send[In <: AmazonWebServiceRequest, Out](aws: In, func: AsyncHandler[In, Out] => juc.Future[Out])(implicit
      d: Describe[_ >: In]): Future[Out] = {

    def name = d.desc(aws)

    def sendSingle(): Future[Out] = {
      val p = Promise[Out]

      val handler = new AsyncHandler[In, Out] {
        override def onError(ex: Exception) =
          ex match {
            case e: ProvisionedThroughputExceededException =>
              p.tryFailure(ex)
            case _ =>
              val n = name
              log.error(ex, "failure while executing {}", n)
              p.tryFailure(new DynamoDBJournalFailure("failure while executing " + n, ex))
          }
        override def onSuccess(req: In, resp: Out) = p.trySuccess(resp)
      }

      try {
        func(handler)
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
      val key = i.get(Key) match {
        case null => "<none>"
        case x    => x.getS
      }
      val sort = i.get(Sort) match {
        case null => "<none>"
        case x    => x.getN
      }
      s"[$Key=$key,$Sort=$sort]"
    }
  }

  object Describe {
    implicit object GenericDescribe extends Describe[AmazonWebServiceRequest] {
      def desc(aws: AmazonWebServiceRequest): String = aws.getClass.getSimpleName
    }
  }

  implicit object DescribeDescribe extends Describe[DescribeTableRequest] {
    def desc(aws: DescribeTableRequest): String = s"DescribeTableRequest(${aws.getTableName})"
  }

  implicit object QueryDescribe extends Describe[QueryRequest] {
    def desc(aws: QueryRequest): String = s"QueryRequest(${aws.getTableName},${aws.getExpressionAttributeValues})"
  }

  implicit object PutItemDescribe extends Describe[PutItemRequest] {
    def desc(aws: PutItemRequest): String = s"PutItemRequest(${aws.getTableName},${formatKey(aws.getItem)})"
  }

  implicit object DeleteDescribe extends Describe[DeleteItemRequest] {
    def desc(aws: DeleteItemRequest): String = s"DeleteItemRequest(${aws.getTableName},${formatKey(aws.getKey)})"
  }

  implicit object BatchGetItemDescribe extends Describe[BatchGetItemRequest] {
    def desc(aws: BatchGetItemRequest): String = {
      val entry = aws.getRequestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys  = entry.getValue.getKeys.asScala.map(formatKey)
      s"BatchGetItemRequest($table, ${keys.mkString("(", ",", ")")})"
    }
  }

  implicit object BatchWriteItemDescribe extends Describe[BatchWriteItemRequest] {
    def desc(aws: BatchWriteItemRequest): String = {
      val entry = aws.getRequestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys = entry.getValue.asScala.map { write =>
        write.getDeleteRequest match {
          case null => "put" + formatKey(write.getPutRequest.getItem)
          case del  => "del" + formatKey(del.getKey)
        }
      }
      s"BatchWriteItemRequest($table, ${keys.mkString("(", ",", ")")})"
    }
  }

  def listTables(aws: ListTablesRequest): Future[ListTablesResult] =
    send[ListTablesRequest, ListTablesResult](aws, dynamoDB.listTablesAsync(aws, _))

  def describeTable(aws: DescribeTableRequest): Future[DescribeTableResult] =
    send[DescribeTableRequest, DescribeTableResult](aws, dynamoDB.describeTableAsync(aws, _))

  def createTable(aws: CreateTableRequest): Future[CreateTableResult] =
    send[CreateTableRequest, CreateTableResult](aws, dynamoDB.createTableAsync(aws, _))

  def updateTable(aws: UpdateTableRequest): Future[UpdateTableResult] =
    send[UpdateTableRequest, UpdateTableResult](aws, dynamoDB.updateTableAsync(aws, _))

  def deleteTable(aws: DeleteTableRequest): Future[DeleteTableResult] =
    send[DeleteTableRequest, DeleteTableResult](aws, dynamoDB.deleteTableAsync(aws, _))

  def query(aws: QueryRequest): Future[QueryResult] =
    send[QueryRequest, QueryResult](aws, dynamoDB.queryAsync(aws, _))

  def scan(aws: ScanRequest): Future[ScanResult] =
    send[ScanRequest, ScanResult](aws, dynamoDB.scanAsync(aws, _))

  def putItem(aws: PutItemRequest): Future[PutItemResult] =
    send[PutItemRequest, PutItemResult](aws, dynamoDB.putItemAsync(aws, _))

  def getItem(aws: GetItemRequest): Future[GetItemResult] =
    send[GetItemRequest, GetItemResult](aws, dynamoDB.getItemAsync(aws, _))

  def updateItem(aws: UpdateItemRequest): Future[UpdateItemResult] =
    send[UpdateItemRequest, UpdateItemResult](aws, dynamoDB.updateItemAsync(aws, _))

  def deleteItem(aws: DeleteItemRequest): Future[DeleteItemResult] =
    send[DeleteItemRequest, DeleteItemResult](aws, dynamoDB.deleteItemAsync(aws, _))

  def batchWriteItem(aws: BatchWriteItemRequest): Future[BatchWriteItemResult] =
    send[BatchWriteItemRequest, BatchWriteItemResult](aws, dynamoDB.batchWriteItemAsync(aws, _))

  def batchGetItem(aws: BatchGetItemRequest): Future[BatchGetItemResult] =
    send[BatchGetItemRequest, BatchGetItemResult](aws, dynamoDB.batchGetItemAsync(aws, _))

}
