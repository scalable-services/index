organization := "services.scalable"
name := "index"

version := "master"

scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",

  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "slf4j-api" % "1.7.30",

  "com.google.guava" % "guava" % "27.1-jre",
  "org.apache.commons" % "commons-lang3" % "3.8.1",

  "com.google.guava" % "guava" % "30.1-jre",

  "com.github.ben-manes.caffeine" % "caffeine" % "2.8.8",

  "com.datastax.oss" % "java-driver-core" % "4.11.1",

  "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6"
)

enablePlugins(AkkaGrpcPlugin)