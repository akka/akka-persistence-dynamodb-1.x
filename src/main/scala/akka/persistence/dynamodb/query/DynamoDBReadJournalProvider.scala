package akka.persistence.dynamodb.query

import akka.actor.ExtendedActorSystem
import akka.persistence.dynamodb.query.javadsl.{ DynamoDBReadJournal => JavaDynamoDBReadJournal }
import akka.persistence.dynamodb.query.scaladsl.internal.{ DynamoDBReadJournal => ScalaDynamoDBReadJournal }
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.javadsl.{ ReadJournal => JavaReadJournal }
import akka.persistence.query.scaladsl.{ ReadJournal => ScalaReadJournal }
import com.typesafe.config.Config

class DynamoDBReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {
  private lazy val _scalaReadJournal                   = new ScalaDynamoDBReadJournal(config, configPath)(system)
  override def scaladslReadJournal(): ScalaReadJournal = _scalaReadJournal

  private lazy val _javadslReadJournal               = new JavaDynamoDBReadJournal(_scalaReadJournal)
  override def javadslReadJournal(): JavaReadJournal = _javadslReadJournal
}

trait ReadJournalSettingsProvider {
  protected def readJournalSettings: DynamoDBReadJournalConfig
}
