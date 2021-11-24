/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import akka.actor.{ ActorSystem, Scheduler }
import akka.event.{ Logging, LoggingAdapter }
import akka.persistence.dynamodb.journal.DynamoDBHelper
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, AttributeValueUpdate }
import java.util.{ Map => JMap }
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

package object dynamodb {
  type Item        = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  def S(value: String): AttributeValue = new AttributeValue().withS(value)

  def N(value: Long): AttributeValue   = new AttributeValue().withN(value.toString)
  def N(value: String): AttributeValue = new AttributeValue().withN(value)
  val Naught                           = N(0)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def lift[T](f: Future[T]): Future[Try[T]] = {
    val p = Promise[Try[T]]
    f.onComplete(p.success)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    p.future
  }

  def liftUnit(f: Future[Any]): Future[Try[Unit]] = {
    val p = Promise[Try[Unit]]
    f.onComplete {
      case Success(_)     => p.success(Success(()))
      case f @ Failure(_) => p.success(f.asInstanceOf[Failure[Unit]])
    }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    p.future
  }

  def trySequence[A, M[X] <: TraversableOnce[X]](in: M[Future[A]])(implicit
      cbf: CanBuildFrom[M[Future[A]], Try[A], M[Try[A]]],
      executor: ExecutionContext): Future[M[Try[A]]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      val fb = lift(a)
      for (r <- fr; b <- fb) yield (r += b)
    }.map(_.result())

  def dynamoClient(system: ActorSystem, settings: DynamoDBConfig): DynamoDBHelper = {
    val client =
      if (settings.AwsKey.nonEmpty && settings.AwsSecret.nonEmpty) {
        val conns    = settings.client.config.getMaxConnections
        val executor = Executors.newFixedThreadPool(conns)
        val creds    = new BasicAWSCredentials(settings.AwsKey, settings.AwsSecret)
        new AmazonDynamoDBAsyncClient(creds, settings.client.config, executor)
      } else {
        new AmazonDynamoDBAsyncClient(settings.client.config)
      }
    client.setEndpoint(settings.Endpoint)
    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    class DynamoDBClient(
        override val ec: ExecutionContext,
        override val dynamoDB: AmazonDynamoDBAsyncClient,
        override val settings: DynamoDBConfig,
        override val scheduler: Scheduler,
        override val log: LoggingAdapter)
        extends DynamoDBHelper

    new DynamoDBClient(dispatcher, client, settings, system.scheduler, Logging(system, "DynamoDBClient"))
  }

}
