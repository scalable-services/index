organization := "services.scalable"
name := "index"

version := "0.33"

scalaVersion := "2.13.12"

lazy val root = (project in file("."))

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",

  "ch.qos.logback" % "logback-classic" % "1.2.10",
  "org.slf4j" % "slf4j-api" % "1.7.33",

  "com.google.guava" % "guava" % "27.1-jre",
  "org.apache.commons" % "commons-lang3" % "3.8.1",

  "com.google.guava" % "guava" % "30.1-jre",

  "com.github.ben-manes.caffeine" % "caffeine" % "2.8.8",

  "com.datastax.oss" % "java-driver-core" % "4.13.0",

  "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6",

  "org.apache.commons" % "commons-compress" % "1.21",

  "org.cassandraunit" % "cassandra-unit" % "4.3.1.0" % Test
)


libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"

//addCommandAlias("history-test", "testOnly services.scalable.index.HistorySpec")
addCommandAlias("main-test", "testOnly services.scalable.index.MainSpec")

enablePlugins(AkkaGrpcPlugin)

// Run in a separate JVM, to make sure sbt waits until all threads have
// finished before returning.
// If you want to keep the application running while executing other
// sbt tasks, consider https://github.com/spray/sbt-revolver/
ThisBuild / fork := true
ThisBuild / run / fork := true
ThisBuild / Test / fork := true

import sbtprotoc.ProtocPlugin._
//ProtobufConfig / javaSource := (Compile / sourceDirectory) / "generated"