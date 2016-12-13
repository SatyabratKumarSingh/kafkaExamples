name := "KafkaExamples"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"
libraryDependencies ++= Seq(
  "org.apache.kafka"           % "kafka_2.11"           % "0.8.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1")
