/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka

import sbt._
import sbt.Keys._
import java.io.File
import sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction

object Publish extends AutoPlugin {

  val defaultPublishTo = settingKey[File]("Default publish directory")

  override def trigger  = allRequirements
  override def requires = sbtrelease.ReleasePlugin

  override lazy val projectSettings = Seq(
    crossPaths := false,
    pomExtra := akkaPomExtra,
    publishTo := akkaPublishTo.value,
    credentials ++= akkaCredentials,
    organization := "com.typesafe.akka",
    organizationName := "Typesafe Inc.",
    organizationHomepage := Some(url("http://www.typesafe.com")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    homepage := Some(url("https://github.com/akka/akka-persistence-dynamodb-1.x")),
    publishMavenStyle := true,
    pomIncludeRepository := { x => false },
    defaultPublishTo := crossTarget.value / "repository")

  def akkaPomExtra = {
    <developers>
      <developer>
        <id>contributors</id>
        <name>Contributors</name>
        <email>akka-dev@googlegroups.com</email>
        <url>https://github.com/akka/akka-persistence-dynamodb-1.x/graphs/contributors</url>
      </developer>
    </developers>
  }

  private def akkaPublishTo =
    Def.setting {
      sonatypeRepo(version.value).orElse(localRepo(defaultPublishTo.value))
    }

  private def sonatypeRepo(version: String): Option[Resolver] =
    Option(sys.props("publish.maven.central")).filter(_.toLowerCase == "true").map { _ =>
      val nexus = "https://oss.sonatype.org/"
      if (version.endsWith("-SNAPSHOT")) "snapshots".at(nexus + "content/repositories/snapshots")
      else "releases".at(nexus + "service/local/staging/deploy/maven2")
    }

  private def localRepo(repository: File) =
    Some(Resolver.file("Default Local Repository", repository))

  private def akkaCredentials: Seq[Credentials] =
    Option(System.getProperty("akka.publish.credentials", null)).map(f => Credentials(new File(f))).toSeq

}
