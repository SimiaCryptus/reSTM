name := """restm-play-demo"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test,
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.simiacryptus" %% "restm-core" % "1.0.1-SNAPSHOT",
  "com.simiacryptus" %% "restm-api" % "1.0.1-SNAPSHOT",
  "com.simiacryptus" %% "restm-ml" % "1.0.1-SNAPSHOT"
)



fork in run := true