/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.nio.ByteBuffer
import java.util.{ Collections, HashMap => JHMap, List => JList, Map => JMap }
import java.util.function.Consumer

import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{ Failure, Success }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import java.util.ArrayList

import akka.actor.ExtendedActorSystem
import akka.stream.stage._
import akka.stream._
import akka.persistence.dynamodb._
import akka.serialization.{ AsyncSerializer, Serialization }
import akka.util.ByteString
import software.amazon.awssdk.services.dynamodb.model._

object DynamoDBRecovery {
  case class ReplayBatch(items: Seq[Item], map: Map[AttributeValue, Long]) {
    def sorted: immutable.Iterable[Item] =
      items.foldLeft(immutable.TreeMap.empty[Long, Item])((acc, i) =>
        acc.updated(itemToSeq(i), i))
        .map(_._2)
    def ids: Seq[Long] = items.map(itemToSeq).sorted
    private def itemToSeq(i: Item): Long = map(i.get(Key)) * 100 + i.get(Sort).n().toInt
  }
}

object RemoveIncompleteAtoms extends GraphStage[FlowShape[Item, List[Item]]] {
  private final val NoBatch = -1L

  val in = Inlet[Item]("RIA.in")
  val out = Outlet[List[Item]]("RIA.out")

  override val shape = FlowShape(in, out)
  override val initialAttributes = Attributes.name("RemoveIncompleteAtoms")

  override def createLogic(attr: Attributes) = new GraphStageLogic(shape) with InHandler with OutHandler {
    var batchEnd = NoBatch
    var batch = List.empty[Item]

    setHandler(out, this)
    setHandler(in, this)

    override def onPull(): Unit = pull(in)

    override def onPush(): Unit = {
      val item = grab(in)
      if (item.containsKey(AtomEnd)) {
        val end = item.get(AtomEnd).n().toLong
        val index = item.get(AtomIndex).n().toLong
        val seqNr = sn(item)
        val myBatchEnd = seqNr - index + end
        if (seqNr == batchEnd) {
          val result =
            if (myBatchEnd == batchEnd) {
              val r = (item :: batch).reverse
              batch = Nil
              batchEnd = NoBatch
              r
            } else {
              // foul play detected, scrap this batch
              batch = item :: Nil
              batchEnd = myBatchEnd
              Nil
            }
          if (result.size == (end + 1)) push(out, result)
          else pull(in)
        } else if (batchEnd == NoBatch || seqNr > batchEnd) {
          batchEnd = myBatchEnd
          batch = item :: Nil
          pull(in)
        } else {
          if (batchEnd == myBatchEnd) batch ::= item
          else {
            batchEnd = myBatchEnd
            batch = item :: Nil
          }
          pull(in)
        }
      } else {
        push(out, item :: Nil)
        // throw away possible incomplete batch
        batchEnd = NoBatch
        batch = Nil
      }
    }

    private def sn(item: Item): Long = {
      val s = item.get(Key).s()
      val n = item.get(Sort).n().toLong
      val pos = s.lastIndexOf('-')
      require(pos != -1, "unknown key format " + s)
      s.substring(pos + 1).toLong * 100 + n
    }

  }
}

trait DynamoDBRecovery extends AsyncRecovery { this: DynamoDBJournal =>
  import DynamoDBRecovery._
  import settings._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(ReplayDispatcher)

  override def asyncReplayMessages(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long,
    max:            Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    logFailure(s"replay for $persistenceId ($fromSequenceNr to $toSequenceNr)") {
      log.debug("starting replay for {} from {} to {} (max {})", persistenceId, fromSequenceNr, toSequenceNr, max)
      // toSequenceNr is already capped to highest and guaranteed to be no less than fromSequenceNr
      readSequenceNr(persistenceId, highest = false).flatMap { lowest =>
        val start = Math.max(fromSequenceNr, lowest)
        val async = ReplayParallelism > 1
        Source(start to toSequenceNr)
          .grouped(MaxBatchGet)
          .mapAsync(ReplayParallelism)(batch => getReplayBatch(persistenceId, batch).map(_.sorted))
          .mapConcat(identity)
          .take(max)
          .via(RemoveIncompleteAtoms)
          .mapConcat(identity)
          .mapAsync(ReplayParallelism)(readPersistentRepr(_, async))
          .runFold(0) { (count, next) => replayCallback(next); count + 1 }
          .map(count => log.debug("replay finished for {} with {} events", persistenceId, count))
      }
    }

  def getReplayBatch(persistenceId: String, seqNrs: Seq[Long]): Future[ReplayBatch] = {
    val batchKeys = seqNrs.map(s => messageKey(persistenceId, s) -> (s / 100))
    val keyAttr = KeysAndAttributes.builder()
      .keys(batchKeys.map(_._1).asJava)
      .consistentRead(true)
      .attributesToGet(Key, Sort, PersistentId, SequenceNr, Payload, Event, Manifest, SerializerId, SerializerManifest, WriterUuid, AtomEnd, AtomIndex)
      .build()
    val get = batchGetReq(Collections.singletonMap(JournalTable, keyAttr))
    dynamo.batchGetItem(get).flatMap(getUnprocessedItems(_)).map {
      result =>
        ReplayBatch(
          result.responses().get(JournalTable).asScala.toSeq,
          batchKeys.iterator.map(p => p._1.get(Key) -> p._2).toMap)
    }
  }

  def listAllSeqNr(persistenceId: String): Future[Seq[Long]] =
    Source.fromIterator { () => Iterator.iterate(0L)(_ + 1) }
      .grouped(MaxBatchGet)
      .mapAsync(ReplayParallelism)(batch => getReplayBatch(persistenceId, batch).map(_.ids))
      .takeWhile(_.nonEmpty)
      .runFold(Vector.empty[Long])(_ ++ _)

  def readSequenceNr(persistenceId: String, highest: Boolean): Future[Long] = {
    if (Tracing) log.debug("readSequenceNr(highest={}, persistenceId={})", highest, persistenceId)
    val keyGroups = readSequenceNrBatches(persistenceId, highest)
      .map(_.map(getMaxSeqNr).recover { case ex: Throwable => -1L })
    Future.sequence(keyGroups).flatMap { seq =>
      seq.max match {
        case -1L =>
          val highOrLow = if (highest) "highest" else "lowest"
          throw new DynamoDBJournalFailure(s"cannot read $highOrLow sequence number for persistenceId $persistenceId")
        case start =>
          if (highest) {
            /*
             * When reading the highest sequence number the stored value always points to the Sort=0 entry
             * for which it was written, all other entries do not update the highest value. Therefore we
             * must scan the partition of this Sort=0 entry and find the highest occupied number.
             */
            val request = eventQuery(persistenceId, start)
            dynamo.query(request)
              .flatMap(getRemainingQueryItems(request, _))
              .flatMap { result =>
                if (result.items().isEmpty) {
                  /*
                   * If this comes back empty then that means that all events have been deleted. The only
                   * reliable way to obtain the previously highest number is to also read the lowest number
                   * (which is always stored in full), knowing that it will be either highest-1 or zero.
                   */
                  readSequenceNr(persistenceId, highest = false).map { lowest =>
                    val ret = Math.max(start, lowest - 1)
                    log.debug("readSequenceNr(highest=true persistenceId={}) = {}", persistenceId, ret)
                    ret
                  }
                } else {
                  /*
                   * `start` is the Sort=0 entry’s sequence number, so add the maximum sort key.
                   */
                  val ret = start + result.items().asScala.map(_.get(Sort).n().toLong).max
                  log.debug("readSequenceNr(highest=true persistenceId={}) = {}", persistenceId, ret)
                  Future.successful(ret)
                }
              }
          } else {
            log.debug("readSequenceNr(highest=false persistenceId={}) = {}", persistenceId, start)
            Future.successful(start)
          }
      }
    }
  }

  def readAllSequenceNr(persistenceId: String, highest: Boolean): Future[Set[Long]] =
    Future.sequence(
      readSequenceNrBatches(persistenceId, highest)
        .map(_.map(getAllSeqNr).recover { case ex: Throwable => Nil }))
      .map(_.flatten.toSet)

  def readSequenceNrBatches(persistenceId: String, highest: Boolean): Iterator[Future[BatchGetItemResponse]] =
    (0 until SequenceShards)
      .iterator
      .map(l => if (highest) highSeqKey(persistenceId, l) else lowSeqKey(persistenceId, l))
      .grouped(MaxBatchGet)
      .map { keys =>
        val ka = KeysAndAttributes.builder().keys(keys.asJava).consistentRead(true).build()
        val get = batchGetReq(Collections.singletonMap(JournalTable, ka))
        dynamo.batchGetItem(get).flatMap(getUnprocessedItems(_))
      }

  private def getMaxSeqNr(resp: BatchGetItemResponse): Long =
    if (resp.responses().isEmpty) 0L
    else {
      var ret = 0L
      resp.responses().get(JournalTable).forEach(new Consumer[Item] {
        override def accept(item: Item): Unit = {
          val seq = item.get(SequenceNr) match {
            case null => 0L
            case attr => attr.n().toLong
          }
          if (seq > ret) ret = seq
        }
      })
      ret
    }

  private def getAllSeqNr(resp: BatchGetItemResponse): Seq[Long] =
    if (resp.responses().isEmpty()) Nil
    else {
      var ret: List[Long] = Nil
      resp.responses().get(JournalTable).forEach(new Consumer[Item] {
        override def accept(item: Item): Unit = {
          item.get(SequenceNr) match {
            case null =>
            case attr => ret ::= attr.n().toLong
          }
        }
      })
      ret
    }

  private def getValueOrEmptyString(item: JMap[String, AttributeValue], key: String): String = {
    if (item.containsKey(key)) item.get(key).s() else ""
  }

  def readPersistentRepr(item: JMap[String, AttributeValue], async: Boolean): Future[PersistentRepr] = {
    val clazz = classOf[PersistentRepr]

    if (item.containsKey(Event)) {
      val serializerManifest = getValueOrEmptyString(item, SerializerManifest)

      val pI = item.get(PersistentId).s()
      val sN = item.get(SequenceNr).n().toLong
      val wU = item.get(WriterUuid).s()
      val reprManifest = getValueOrEmptyString(item, Manifest)

      val eventPayload = item.get(Event).b()
      val serId = item.get(SerializerId).n().toInt

      val fut = serialization.serializerByIdentity.get(serId) match {
        case Some(asyncSerializer: AsyncSerializer) =>
          Serialization.withTransportInformation(context.system.asInstanceOf[ExtendedActorSystem]) { () =>
            asyncSerializer.fromBinaryAsync(eventPayload.asByteArray(), serializerManifest)
          }
        case _ =>
          def deserializedEvent: AnyRef = {
            // Serialization.deserialize adds transport info
            serialization.deserialize(eventPayload.asByteArray(), serId, serializerManifest).get
          }
          if (async) Future(deserializedEvent)
          else
            Future.successful(deserializedEvent)
      }

      fut.map { event: AnyRef =>
        PersistentRepr(
          event,
          sequenceNr    = sN,
          persistenceId = pI,
          manifest      = reprManifest,
          writerUuid    = wU,
          sender        = null)
      }

    } else {

      def deserializedEvent: PersistentRepr = {
        // Serialization.deserialize adds transport info
        serialization.deserialize(item.get(Payload).b().asByteArray(), clazz).get
      }

      if (async) Future(deserializedEvent)
      else
        Future.successful(deserializedEvent)

    }
  }

  def getUnprocessedItems(result: BatchGetItemResponse, retriesRemaining: Int = 10): Future[BatchGetItemResponse] = {
    val unprocessed = result.unprocessedKeys().get(JournalTable) match {
      case null => 0
      case x    => x.keys().size
    }
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      Future.failed(new DynamoDBJournalFailure(s"unable to batch get ${result.unprocessedKeys().get(JournalTable).keys()} after 10 tries"))
    } else {
      val rest = batchGetReq(result.unprocessedKeys())
      dynamo.batchGetItem(rest).map { rr =>
        val items = rr.responses().get(JournalTable)
        val responses = result.responses().get(JournalTable)
        items.forEach(new Consumer[Item] {
          override def accept(item: Item): Unit = responses.add(item)
        })

        result.toBuilder.unprocessedKeys(rr.unprocessedKeys()).build()

      }.flatMap(getUnprocessedItems(_, retriesRemaining - 1))
    }
  }

  def getRemainingQueryItems(request: QueryRequest, result: QueryResponse): Future[QueryResponse] = {
    val last = result.lastEvaluatedKey()
    if (last == null || last.isEmpty || last.get(Sort).n().toLong == 99) Future.successful(result)
    else {
      dynamo.query(request.toBuilder().exclusiveStartKey(last).build()).map { next =>
        val merged = new ArrayList[Item](result.items().size + next.items().size)
        merged.addAll(result.items())
        merged.addAll(next.items())
        next.toBuilder().items(merged).build()
      }
    }
  }

  def eventQuery(persistenceId: String, sequenceNr: Long) =
    QueryRequest.builder()
      .tableName(JournalTable)
      .keyConditionExpression(Key + " = :kkey")
      .expressionAttributeValues(Collections.singletonMap(":kkey", S(messagePartitionKey(persistenceId, sequenceNr))))
      .projectionExpression("num")
      .consistentRead(true)
      .build()

  def batchGetReq(items: JMap[String, KeysAndAttributes]) =
    BatchGetItemRequest.builder()
      .requestItems(items)
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()
}
