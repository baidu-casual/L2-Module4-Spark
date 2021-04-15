name := "spark_streaming"

version := "1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.1"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
//libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.7.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-avro
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.1.1"

// https://mvnrepository.com/artifact/io.delta/delta-core
libraryDependencies += "io.delta" %% "delta-core" % "0.8.0"

// libraryDependencies ++= Seq(
//   "org.apache.hadoop" % "hadoop-client" % "2.7.2",
//   "org.apache.parquet" % "parquet-hadoop" % "1.10.1",
//   "io.delta" %% "delta-standalone" % "0.2.0"
// )