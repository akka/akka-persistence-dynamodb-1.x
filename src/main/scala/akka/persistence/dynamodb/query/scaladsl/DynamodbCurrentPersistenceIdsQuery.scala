package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.persistence.dynamodb.query.ReadJournalSettingsProvider
import akka.persistence.dynamodb.{ActorSystemProvider, DynamoProvider, LoggingProvider}
import akka.persistence.dynamodb.query.scaladsl.DynamodbCurrentPersistenceIdsQuery.{RichNumber, RichOption, RichScanResult, SourceLazyOps}
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest, ScanResult}
import akka.util.ccompat.JavaConverters._

import scala.concurrent.Future
import scala.util.control.NonFatal

trait DynamodbCurrentPersistenceIdsQuery extends CurrentPersistenceIdsQuery { self: ReadJournalSettingsProvider with DynamoProvider with ActorSystemProvider with LoggingProvider =>

  /**
   * Same type of query as [[akka.persistence.query.scaladsl.PersistenceIdsQuery.persistenceIds()]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   *
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] = {
    log.debug("starting currentPersistenceIds")
    currentPersistenceIdsByPageInternal()
      .mapConcat(seq => seq.toList)
      .log("currentPersistenceIds")
  }

  /**
   * The implementation that is used for [[currentPersistenceIds]]
   * Here the results are offered page by page.
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  def currentPersistenceIdsByPage(): Source[Seq[String], NotUsed] = {
    log.debug("starting currentPersistenceIdsByPage")
    currentPersistenceIdsByPageInternal()
      .log("currentPersistenceIdsByPage")
  }

  private def currentPersistenceIdsByPageInternal(): Source[Seq[String], NotUsed] = {
    import system.dispatcher
    type ResultSource = Source[Option[ScanResult], NotUsed]

    def nextCall(maybePreviousResult: Option[ScanResult]) =
      maybePreviousResult match {
        case Some(previousResult) if previousResult.hasNextResult => dynamo.scan(scanRequest(Some(previousResult.getLastEvaluatedKey))).map(Some(_))
        case Some(previousResult) if !previousResult.hasNextResult => Future.successful(None)
        case None => Future.successful(None)
      }

    def lazyStream(currentResult: ResultSource): ResultSource = {
      def nextResult: ResultSource = currentResult.mapAsync(parallelism = 1)(nextCall)
      currentResult.concatLazy(lazyStream(nextResult))
    }

    val infiniteStreamOfResults: ResultSource =
      lazyStream(Source.fromFuture(dynamo.scan(scanRequest(None)).map(Some(_))))

    infiniteStreamOfResults
      .takeWhile(_.isDefined)
      .flatMapConcat(_.toSource)
      .map(scanResult =>
        scanResult.toPersistenceIdsPage
          .map ( rawPersistenceId => parsePersistenceId(rawPersistenceId = rawPersistenceId, journalName = readJournalSettings.JournalName) )
      )
  }

  private def scanRequest(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]): ScanRequest = {
    val req = new ScanRequest()
      .withTableName(readJournalSettings.Table)
      .withProjectionExpression("par")
      .withFilterExpression("num = :n")
      .withExpressionAttributeValues(Map(":n" -> 1.toAttribute).asJava)
    exclusiveStartKey.foreach(esk => req.withExclusiveStartKey(esk))
    req
  }

  // persistence id is formatted as follows journal-P-98adb33a-a94d-4ec8-a279-4570e16a0c14-0
  // see DynamoDBJournal.messagePartitionKeyFromGroupNr
  private def parsePersistenceId(rawPersistenceId: String, journalName: String): String =
    try {
      val prefixLength = journalName.length + 3
      val startPostfix = rawPersistenceId.lastIndexOf("-")
      rawPersistenceId.substring(prefixLength, startPostfix)
    } catch {
      case NonFatal(_) =>
        log.error("Could not parse raw persistence id '{}' using journal name '{}'. Returning it unparsed.", rawPersistenceId, journalName)
        rawPersistenceId
    }
}

object DynamodbCurrentPersistenceIdsQuery {
  implicit class RichString(val s: String) extends AnyVal {
    def toAttribute: AttributeValue = new AttributeValue().withS(s)
  }

  implicit class RichNumber(val n: Int) extends AnyVal {
    def toAttribute: AttributeValue = new AttributeValue().withN(n.toString)
  }

  implicit class RichOption[+A](val option: Option[A]) extends AnyVal {
    def toSource: Source[A, NotUsed] = option match {
        case Some(value) => Source.single(value)
        case None => Source.empty
      }
  }

  implicit class RichScanResult(val scanResult: ScanResult) extends AnyVal {
    def toPersistenceIdsPage: Seq[String] = scanResult.getItems.asScala.map(item => item.get("par").getS).toList

    def hasNextResult: Boolean = scanResult.getLastEvaluatedKey != null && !scanResult.getLastEvaluatedKey.isEmpty
  }

  implicit class SourceLazyOps[E, M](val src: Source[E, M]) {

    // see https://github.com/akka/akka/issues/23044
    // when migrating to akka 2.6.x use akka's concatLazy
    def concatLazy[M1](src2: => Source[E, M1]): Source[E, NotUsed] =
      Source(List(() => src, () => src2)).flatMapConcat(_ ())
  }
}
