name := "spark-streaming-kafka-offset-mysql"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.0.4" % "test",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "mysql" % "mysql-connector-java" % "5.1.44",
  "org.apache.spark" % "spark-core_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.0",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.0"
)