name := "spark-serial"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "0.9.0-incubating",
  "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating",
  "org.scalaz.stream" %% "scalaz-stream" % "0.3.1",
  "nl.grons" %% "metrics-scala" % "3.0.4",
  "org.scalatest" %% "scalatest" % "2.0" % "test"
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xlog-implicits")

