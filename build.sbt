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
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.4"
)



fork in run := true