name := "akka-persistence-dynamodb"

scalaVersion       := "2.12.11"
crossScalaVersions := Seq("2.11.12", "2.12.11", "2.13.3")
crossVersion       := CrossVersion.binary

val akkaVersion = "2.5.29"
val amzVersion = "1.11.602"

libraryDependencies ++= Seq(
  "com.amazonaws"       % "aws-java-sdk-core"       % amzVersion,
  "com.amazonaws"       % "aws-java-sdk-dynamodb"   % amzVersion,
  "com.typesafe.akka"   %% "akka-persistence"       % akkaVersion,
  "com.typesafe.akka"   %% "akka-stream"            % akkaVersion,
  "com.typesafe.akka"   %% "akka-persistence-tck"   % akkaVersion   % "test",
  "com.typesafe.akka"   %% "akka-testkit"           % akkaVersion   % "test",
  "org.scalatest"       %% "scalatest"              % "3.0.8"       % "test",
  "commons-io"          % "commons-io"              % "2.4"         % "test",
  "org.hdrhistogram"    % "HdrHistogram"            % "2.1.8"       % "test"
)

parallelExecution in Test := false
logBuffered := false
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

import com.typesafe.sbt.SbtScalariform.ScalariformKeys

ScalariformKeys.autoformat := true
ScalariformKeys.preferences in Compile  := formattingPreferences
ScalariformKeys.preferences in Test     := formattingPreferences

def formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(SpacesAroundMultiImports, true)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(AlignArguments, true)
}
