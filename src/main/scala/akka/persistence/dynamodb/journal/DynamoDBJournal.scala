/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.nio.ByteBuffer
import java.util.{ HashMap => JHMap, Map => JMap }

import akka.Done
import akka.actor.{ ActorLogging, ActorRefFactory, ActorSystem }
import akka.event.{ Logging, LoggingAdapter }
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, Persistence, PersistentRepr }
import akka.serialization.{ AsyncSerializer, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NoStackTrace
import akka.actor.ActorRef

import scala.concurrent.Promise
import akka.persistence.dynamodb._

class DynamoDBJournalFailure(message: String, cause: Throwable = null)   extends RuntimeException(message, cause)
class DynamoDBJournalRejection(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/**
 * Query the table for all sequence numbers of the given persistenceId, starting
 * from zero upwards and finishing when encountering a run of at least MaxBatchGet
 * missing entries. This is a potentially very expensive operation, use with care!
 *
 * A response of type [[ListAllResult]] will be sent back to the given `replyTo`
 * reference.
 */
case class ListAll(persistenceId: String, replyTo: ActorRef)

/**
 * Response to the [[ListAll]] request, containing
 *
 *  - the persistenceId
 *  - the set of lowest sequence numbers stored in the sequence shards
 *  - the set of highest sequence numbers stored in the sequence shards
 *  - the sequence numbers of all stored events in ascending order
 *
 * The lowest/highest sequence number is obtained by taking the maximum of either set.
 */
case class ListAllResult(persistenceId: String, lowest: Set[Long], highest: Set[Long], events: Seq[Long])

/**
 * Purge all information stored for the given `persistenceId` from the journal.
 * Purging the information for a running actor results in undefined behavior.
 *
 * A confirmation of type [[Purged]] will be sent to the given `replyTo` reference.
 */
case class Purge(persistenceId: String, replyTo: ActorRef)

private[akka] case class SetDBHelperReporter(ref: ActorRef)

/**
 * Confirmation that all information stored for the given `persistenceId` has
 * been purged from the journal.
 */
case class Purged(persistenceId: String)

class DynamoDBJournal(config: Config)
    extends AsyncWriteJournal
    with DynamoDBRecovery
    with DynamoDBJournalRequests
    with ActorLogging {
  import context.dispatcher

  implicit val materializer = ActorMaterializer()

  val extension     = Persistence(context.system)
  val serialization = SerializationExtension(context.system)

  val settings = new DynamoDBJournalConfig(config)

  import settings._
  if (LogConfig) log.info("using settings {}", settings)

  val dynamo = dynamoClient(context.system, settings)

  dynamo.describeTable(new DescribeTableRequest().withTableName(JournalTable)).onComplete {
    case Success(result) => log.info("using DynamoDB table {}", result)
    case _               => log.error("persistent actor requests will fail until the table '{}' is accessible", JournalTable)
  }

  override def postStop(): Unit = dynamo.shutdown()

  private case class OpFinished(pid: String, f: Future[Done])
  private val opQueue: JMap[String, Future[Done]] = new JHMap

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val p   = Promise[Done]
    val pid = messages.head.persistenceId
    opQueue.put(pid, p.future)

    val f = writeMessages(messages)

    f.onComplete { _ =>
      self ! OpFinished(pid, p.future)
      p.success(Done)
    }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

    f
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    opQueue.get(persistenceId) match {
      case null =>
        logFailure(s"read-highest-sequence-number($persistenceId)")(readSequenceNr(persistenceId, highest = true))
      case f =>
        f.flatMap(_ => logFailure(s"read-highest($persistenceId)")(readSequenceNr(persistenceId, highest = true)))
    }

  /**
   * Delete messages up to a given sequence number. The range to which this applies
   * first capped by the lowest and highest sequence number for this persistenceId
   * since DynamoDB requires individual deletes to be issued for every single event.
   * The procedure is to first update the lowest sequence number to the new value
   * and then delete the now unreplayable events—this is desirable because in the
   * other order a replay may see partially deleted history.
   *
   * Failures during purging are only logged and do not contribute to the call’s
   * result.
   *
   * TODO in principle replays should be inhibited while the purge is ongoing
   */
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    logFailure(s"delete($persistenceId, upto=$toSequenceNr)") {
      log.debug("delete-messages persistenceId={} to={}", persistenceId, toSequenceNr)
      val lowF  = readSequenceNr(persistenceId, highest = false)
      val highF = readSequenceNr(persistenceId, highest = true)
      for {
        lowest  <- lowF
        highest <- highF
        val upTo = Math.min(toSequenceNr, highest)
        _ <- if (upTo + 1 > lowest) setLS(persistenceId, to = upTo + 1) else Future.successful(Done)
        _ <- if (lowest <= upTo) deleteMessages(persistenceId, lowest, upTo) else Future.successful(Done)
      } yield {
        log.debug("finished asyncDeleteMessagesTo {} {} ({})", persistenceId, toSequenceNr, upTo)
      }
    }

  private def listAll(persistenceId: String): Future[ListAllResult] =
    for {
      low  <- readAllSequenceNr(persistenceId, highest = false)
      high <- readAllSequenceNr(persistenceId, highest = true)
      seqs <- listAllSeqNr(persistenceId)
    } yield ListAllResult(persistenceId, low, high, seqs)

  private def purge(persistenceId: String): Future[Done] =
    for {
      highest <- readSequenceNr(persistenceId, highest = true)
      _       <- deleteMessages(persistenceId, 0, highest)
      _       <- removeLS(persistenceId)
      _       <- removeHS(persistenceId)
    } yield Done

  override def receivePluginInternal = {
    case OpFinished(persistenceId, f)    => opQueue.remove(persistenceId, f)
    case ListAll(persistenceId, replyTo) => listAll(persistenceId).pipeTo(replyTo)
    case Purge(persistenceId, replyTo)   => purge(persistenceId).map(_ => Purged(persistenceId)).pipeTo(replyTo)
    case SetDBHelperReporter(ref)        => dynamo.setReporter(ref)
  }

  def messageKey(persistenceId: String, sequenceNr: Long): Item = {
    val item: Item = new JHMap
    item.put(Key, S(messagePartitionKey(persistenceId, sequenceNr)))
    item.put(Sort, N(sequenceNr % 100))
    item
  }

  def messagePartitionKey(persistenceId: String, sequenceNr: Long): String =
    s"$JournalName-P-$persistenceId-${sequenceNr / 100}"

  def highSeqKey(persistenceId: String, shard: Long) = {
    val item: Item = new JHMap
    item.put(Key, S(s"$JournalName-SH-$persistenceId-$shard"))
    item.put(Sort, Naught)
    item
  }

  def lowSeqKey(persistenceId: String, shard: Long) = {
    val item: Item = new JHMap
    item.put(Key, S(s"$JournalName-SL-$persistenceId-$shard"))
    item.put(Sort, Naught)
    item
  }

  def logFailure[T](desc: String)(f: Future[T]): Future[T] =
    f.transform(
      identity(_),
      ex => {
        log.error(ex, "operation failed: " + desc)
        ex
      })
}
