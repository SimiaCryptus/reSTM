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
  "com.google.code.gson" % "gson" % "2.8.0"
)



fork in run := true