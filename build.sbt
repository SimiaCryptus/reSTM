name := """play-scala"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test,
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.4",
  "com.twitter" %% "chill" % "0.8.1",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.63",
  "com.google.code.gson" % "gson" % "2.8.0",
  "commons-io" % "commons-io" % "2.5",
  "com.sleepycat" % "je" % "5.0.73",
  //"org.apache.commons" % "commons-text" % "0.1-SNAPSHOT",
  "de.unkrig.commons" % "commons-text" % "1.2.7",
  "net.jpountz.lz4" % "lz4" % "1.3.0",
  "ch.qos.logback" % "logback-classic" % "1.1.7"
)



fork in run := true