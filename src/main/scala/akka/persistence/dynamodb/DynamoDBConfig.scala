/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

import com.amazonaws.{ ClientConfiguration, Protocol }
import com.typesafe.config.Config

import java.net.InetAddress

trait ClientConfig {
  val config: ClientConfiguration
}

trait DynamoDBConfig {
  val AwsKey: String
  val AwsSecret: String
  val Endpoint: String
  val ClientDispatcher: String
  val client: ClientConfig
  val Tracing: Boolean
  val MaxBatchGet: Int
  val MaxBatchWrite: Int
  val MaxItemSize: Int
  val Table: String
  val JournalName: String
  val TTLConfig: Option[DynamoDBTTLConfig]
}

class DynamoDBClientConfig(c: Config) extends ClientConfig {
  private val cc = c.getConfig("aws-client-config")
  private def get[T](path: String, extract: (Config, String) => T, set: T => Unit): Unit =
    if (cc.getString(path) == "default") ()
    else {
      val value = extract(cc, path)
      set(value)
      foundSettings ::= s"$path:$value"
    }

  private var foundSettings          = List.empty[String]
  override lazy val toString: String = foundSettings.reverse.mkString("{", ",", "}")
  val config                         = new ClientConfiguration

  get("client-execution-timeout", _.getInt(_), config.setClientExecutionTimeout)
  get("connection-max-idle-millis", _.getLong(_), config.setConnectionMaxIdleMillis)
  get("connection-timeout", _.getInt(_), config.setConnectionTimeout)
  get("connection-ttl", _.getLong(_), config.setConnectionTTL)
  get("local-address", (c, p) => InetAddress.getByName(c.getString(p)), config.setLocalAddress)
  get("max-connections", _.getInt(_), config.setMaxConnections)
  get("max-error-retry", _.getInt(_), config.setMaxErrorRetry)
  get("preemptive-basic-proxy-auth", _.getBoolean(_), config.withPreemptiveBasicProxyAuth)
  get("protocol", (c, p) => if (c.getString(p) == "HTTP") Protocol.HTTP else Protocol.HTTPS, config.setProtocol)
  get("proxy-domain", _.getString(_), config.setProxyDomain)
  get("proxy-host", _.getString(_), config.setProxyHost)
  get("proxy-password", _.getString(_), config.setProxyPassword)
  get("proxy-port", _.getInt(_), config.setProxyPort)
  get("proxy-username", _.getString(_), config.setProxyUsername)
  get("proxy-workstation", _.getString(_), config.setProxyWorkstation)
  get("request-timeout", _.getInt(_), config.setRequestTimeout)
  get("response-metadata-cache-size", _.getInt(_), config.setResponseMetadataCacheSize)
  get("signer-override", _.getString(_), config.setSignerOverride)
  get[(Int, Int)](
    "socket-buffer-size-hints",
    (c, p) => {
      val tuple = c.getIntList(p)
      require(tuple.size == 2, "socket-buffer-size-hints must be a list of two integers")
      (tuple.get(0), tuple.get(1))
    },
    pair => config.setSocketBufferSizeHints(pair._1, pair._2))
  get("socket-timeout", _.getInt(_), config.setSocketTimeout)
  get("use-expect-continue", _.getBoolean(_), config.setUseExpectContinue)
  get("use-gzip", _.getBoolean(_), config.setUseExpectContinue)
  get("use-reaper", _.getBoolean(_), config.setUseReaper)
  get("use-tcp-keepalive", _.getBoolean(_), config.setUseTcpKeepAlive)
  get("user-agent", _.getString(_), config.setUserAgent)
}
