resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.2")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.9")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")

addSbtPlugin("com.lightbend" % "sbt-whitesource"  % "0.1.7")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")

// https://github.com/dwijnand/sbt-dynver
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
