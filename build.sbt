import com.github.play2war.plugin._

name := """import-concepts"""

version := "4.0.0"

Play2WarPlugin.play2WarSettings
Play2WarKeys.servletVersion := "3.0"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  ws,
  "org.scalatestplus" %% "play" % "1.1.0" % Test,
  "io.jvm.uuid" %% "scala-uuid" % "0.2.1",
  "com.netaporter" %% "scala-uri" % "0.4.14",
  "org.scalaj" % "scalaj-time_2.10.2" % "0.7",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
  "com.roundeights" %% "hasher" % "1.0.0",
  "mu-semtech" % "db-support" % "0.2"
)

resolvers ++= Seq(
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
  "RoundEights" at "http://maven.spikemark.net/roundeights")
