package akka.persistence.dynamodb.query.scaladsl.internal

import akka.NotUsed
import akka.persistence.dynamodb.journal.JournalKeys
import akka.persistence.dynamodb.query.scaladsl.internal.DynamoDBCurrentPersistenceIdsQuery.{
  RichNumber,
  RichString,
  SourceLazyOps
}
import akka.persistence.dynamodb.query.scaladsl.internal.PersistenceIdsResult.RichPersistenceIdsResult
import akka.persistence.dynamodb.query.scaladsl.{
  CreatePersistenceIdsIndex,
  DynamoDBCurrentPersistenceIdsQuery => PublicDynamoDBCurrentPersistenceIdsQuery
}
import akka.persistence.dynamodb.query.{ ReadJournalSettingsProvider, RichOption }
import akka.persistence.dynamodb.{ ActorSystemProvider, DynamoProvider, LoggingProvider }
import akka.stream.scaladsl.Source
import akka.util.ccompat.JavaConverters._
import com.amazonaws.services.dynamodbv2.model._

import java.util
import scala.concurrent.Future
import scala.util.control.NonFatal

trait DynamoDBCurrentPersistenceIdsQuery extends PublicDynamoDBCurrentPersistenceIdsQuery {
  self: ReadJournalSettingsProvider
    with DynamoProvider
    with ActorSystemProvider
    with LoggingProvider
    with JournalKeys =>

  /**
   * Same type of query as [[akka.persistence.query.scaladsl.PersistenceIdsQuery.persistenceIds()]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   *
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  override def currentPersistenceIds(): Source[String, NotUsed] = {
    log.debug("starting currentPersistenceIds")
    currentPersistenceIdsQueryInternal().mapConcat(seq => seq.toList).log("currentPersistenceIds")
  }

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  def currentPersistenceIdsByPageScan(): Source[Seq[String], NotUsed] = {
    log.debug("starting currentPersistenceIdsByPageScan")
    currentPersistenceIdsScanInternal().log("currentPersistenceIdsByPageScan")
  }

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  def currentPersistenceIdsByPageQuery(): Source[Seq[String], NotUsed] = {
    log.debug("starting currentPersistenceIdsByPageQuery")
    currentPersistenceIdsQueryInternal().log("currentPersistenceIdsByPageQuery")
  }

  /**
   * Persistence ids are returned alphabetically page by page.
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsAlphabeticallyIndexRequest]]
   */
  def currentPersistenceIdsAlphabeticallyByPageQuery(
      fromPersistenceId: Option[String] = None): Source[Seq[String], NotUsed] = {
    log.debug("starting currentPersistenceIdsAlphabeticallyByPageQuery")
    currentPersistenceIdsQueryInternal(fromPersistenceId).log("currentPersistenceIdsAlphabeticallyByPageQuery")
  }

  private def currentPersistenceIdsScanInternal(): Source[Seq[String], NotUsed] = {
    import PersistenceIdsResult.persistenceIdsScanResult
    currentPersistenceIdsByPageInternal(scanPersistenceIds)
  }

  private def currentPersistenceIdsQueryInternal(
      fromPersistenceId: Option[String] = None): Source[Seq[String], NotUsed] = {
    import PersistenceIdsResult.persistenceIdsQueryResult
    currentPersistenceIdsByPageInternal(queryPersistenceIds(fromPersistenceId = fromPersistenceId))
  }

  private def currentPersistenceIdsByPageInternal[Result: PersistenceIdsResult](
      getPersistenceIds: Option[java.util.Map[String, AttributeValue]] => Future[Result])
      : Source[Seq[String], NotUsed] = {
    import system.dispatcher
    type ResultSource = Source[Option[Result], NotUsed]

    def nextCall(maybePreviousResult: Option[Result]): Future[Option[Result]] = {
      val maybeNextResult = for {
        previousResult   <- maybePreviousResult
        nextEvaluatedKey <- previousResult.nextEvaluatedKey
      } yield getPersistenceIds(Some(nextEvaluatedKey)).map(Some(_))

      maybeNextResult.getOrElse(Future.successful(None))
    }

    def lazyStream(currentResult: ResultSource): ResultSource = {
      def nextResult: ResultSource = currentResult.mapAsync(parallelism = 1)(nextCall)

      currentResult.concatLazy(lazyStream(nextResult))
    }

    val infiniteStreamOfResults: ResultSource =
      lazyStream(Source.fromFuture(getPersistenceIds(None).map(Some(_))))

    infiniteStreamOfResults
      .takeWhile(_.isDefined)
      .flatMapConcat(_.toSource)
      .map(persistenceIdsResult =>
        persistenceIdsResult.toPersistenceIdsPage.flatMap(rawPersistenceId =>
          parsePersistenceId(rawPersistenceId = rawPersistenceId, journalName = readJournalSettings.JournalName)))
  }

  private def queryPersistenceIds(fromPersistenceId: Option[String])(
      exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {

    def queryRequest(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]): QueryRequest = {
      val req = new QueryRequest()
        .withTableName(readJournalSettings.Table)
        .withIndexName(readJournalSettings.PersistenceIdsIndexName)
        .withProjectionExpression("par")

      fromPersistenceId match {
        case Some(persistenceId) =>
          req
            .withKeyConditionExpression("num = :n AND par > :p")
            .withExpressionAttributeValues(
              Map(":n" -> 1.toAttribute, ":p" -> messagePartitionKeyFromGroupNr(persistenceId, 0).toAttribute).asJava)
        case None =>
          req.withKeyConditionExpression("num = :n").withExpressionAttributeValues(Map(":n" -> 1.toAttribute).asJava)

      }
      exclusiveStartKey.foreach(esk => req.withExclusiveStartKey(esk))
      req
    }
    dynamo.query(queryRequest(exclusiveStartKey))
  }

  private def scanPersistenceIds(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {
    def scanRequest(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]): ScanRequest = {
      val req = new ScanRequest()
        .withTableName(readJournalSettings.Table)
        .withProjectionExpression("par")
        .withFilterExpression("num = :n")
        .withExpressionAttributeValues(Map(":n" -> 1.toAttribute).asJava)
      exclusiveStartKey.foreach(esk => req.withExclusiveStartKey(esk))
      req
    }
    dynamo.scan(scanRequest(exclusiveStartKey))
  }

  // persistence id is formatted as follows journal-P-98adb33a-a94d-4ec8-a279-4570e16a0c14-0
  // see DynamoDBJournal.messagePartitionKeyFromGroupNr
  private def parsePersistenceId(rawPersistenceId: String, journalName: String): Option[String] =
    try {
      val prefixLength = journalName.length + 3
      val startPostfix = rawPersistenceId.lastIndexOf("-")
      val postfix      = rawPersistenceId.substring(startPostfix)
      val partitionNr  = postfix.substring(postfix.lastIndexOf("-")).toInt
      if (partitionNr == 0)
        Some(rawPersistenceId.substring(prefixLength, startPostfix))
      else
        None
    } catch {
      case NonFatal(_) =>
        log.error(
          "Could not parse raw persistence id '{}' using journal name '{}'. Returning it unparsed.",
          rawPersistenceId,
          journalName)
        Some(rawPersistenceId)
    }
}

object DynamoDBCurrentPersistenceIdsQuery {
  implicit class RichString(val s: String) extends AnyVal {
    def toAttribute: AttributeValue = new AttributeValue().withS(s)
  }

  implicit class RichNumber(val n: Int) extends AnyVal {
    def toAttribute: AttributeValue = new AttributeValue().withN(n.toString)
  }

  implicit class SourceLazyOps[E, M](val src: Source[E, M]) extends AnyVal {

    // see https://github.com/akka/akka/issues/23044
    // when migrating to akka 2.6.x use akka's concatLazy
    def concatLazy[M1](src2: => Source[E, M1]): Source[E, NotUsed] =
      Source(List(() => src, () => src2)).flatMapConcat(_())
  }
}

// The commonality between QueryResult and ScanResult which don't share an interface
private[query] trait PersistenceIdsResult[A] {
  def toPersistenceIdsPage(result: A): Seq[String]

  def nextEvaluatedKey(result: A): Option[util.Map[String, AttributeValue]]
}

private[query] object PersistenceIdsResult {

  implicit val persistenceIdsQueryResult: PersistenceIdsResult[QueryResult] = new PersistenceIdsResult[QueryResult] {
    override def toPersistenceIdsPage(result: QueryResult): Seq[String] =
      result.getItems.asScala.map(item => item.get("par").getS).toSeq

    override def nextEvaluatedKey(result: QueryResult): Option[util.Map[String, AttributeValue]] =
      if (result.getLastEvaluatedKey != null && !result.getLastEvaluatedKey.isEmpty) Some(result.getLastEvaluatedKey)
      else None
  }

  implicit val persistenceIdsScanResult: PersistenceIdsResult[ScanResult] = new PersistenceIdsResult[ScanResult] {
    override def toPersistenceIdsPage(result: ScanResult): Seq[String] =
      result.getItems.asScala.map(item => item.get("par").getS).toSeq

    override def nextEvaluatedKey(result: ScanResult): Option[util.Map[String, AttributeValue]] =
      if (result.getLastEvaluatedKey != null && !result.getLastEvaluatedKey.isEmpty) Some(result.getLastEvaluatedKey)
      else None
  }

  implicit class RichPersistenceIdsResult[Result](val result: Result) extends AnyVal {
    def toPersistenceIdsPage(implicit persistenceIdsResult: PersistenceIdsResult[Result]): Seq[String] =
      persistenceIdsResult.toPersistenceIdsPage(result)

    def nextEvaluatedKey(implicit
        persistenceIdsResult: PersistenceIdsResult[Result]): Option[util.Map[String, AttributeValue]] =
      persistenceIdsResult.nextEvaluatedKey(result)
  }
}
