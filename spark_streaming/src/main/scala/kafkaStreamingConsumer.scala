package main


import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.streaming.Trigger

/*
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
*/

class sparkStreamng {
  def streamingFunction(batchDf: DataFrame, batchId: Long): Unit = {
        println("\n\n\t\tBATCH "+batchId+"\n\n")
        batchDf.show(false)
    }
  def kafkaConsume(kafkaTopicName: String = "test-events", kafkaServer: String = "localhost:9092"): Unit = {
    val conf = new SparkConf().setAppName("KAFKA").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Kafka Consumer")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    System.setProperty("HADOOP_USER_NAME","hadoop") 
       

    val transactionDF = spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", kafkaTopicName)
                .option("startingOffsets", "earliest")
                .load()

    println("Printing Schema of transactionDF: ")
    transactionDF.printSchema()
    
    
    /**
      * Lamda Function
      */
      /*
    transactionDF.writeStream.foreachBatch((batchDf: DataFrame, batchId: Long) => {
        batchDf.show(false)
    }).start().awaitTermination()*/
    val transactionDFCounts = transactionDF
              .withWatermark("timestamp", "10 minutes")
              //.groupBy(window($"timestamp", "10 minutes", "5 minutes"),$"value")
              //.count()
    

    transactionDFCounts
                .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)")
                .writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("topic", kafkaTopicName)
                .trigger(Trigger.ProcessingTime("1 seconds"))
                .outputMode("update")
                .foreachBatch(streamingFunction _)
                .option("checkpointLocation","/tmp/spark/kafkaStreamingConsumer")
                .start()
                .awaitTermination()
    spark.close()
    
  }
  def kafkaConsumeCSV(kafkaTopicName: String = "test-events", kafkaServer: String = "localhost:9092"): Unit = {
    val conf = new SparkConf().setAppName("KAFKA").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Kafka Consumer")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    System.setProperty("HADOOP_USER_NAME","hadoop") 
       

    val transactionDF = spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", kafkaTopicName)
                .option("startingOffsets", "earliest")
                .load()

    println("Printing Schema of transactionDF: ")
    transactionDF.printSchema()
    
    
    /**
      * Lamda Function
      */
      /*
    transactionDF.writeStream.foreachBatch((batchDf: DataFrame, batchId: Long) => {
        batchDf.show(false)
    }).start().awaitTermination()*/
    val transactionDFCounts = transactionDF
              .withWatermark("timestamp", "10 minutes")
              .groupBy(window($"timestamp", "10 minutes", "5 minutes"),$"value")
              .count()
    

    transactionDFCounts
                .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)")
                .writeStream
                .format("console")
                .trigger(Trigger.ProcessingTime("1 seconds"))
                .outputMode("update")
                .foreachBatch(streamingFunction _)
                .option("checkpointLocation","/tmp/spark/kafkaStreamingConsumer")
                .start()
                .awaitTermination()
    spark.close()
    
  }
  
}


//
object kafkaStreamingConsumer {  
  def main(args: Array[String]): Unit = {
    println("\n\n\t\tKafka Consumer Application Started ...\n\n")
    val sS = new sparkStreamng
    sS.kafkaConsume()
    println("\n\n\t\tKafka Consumer Application Completed ...\n\n")
  }
}



/**
  * val trans_detail_write_stream_1 = transactionDF5
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
  .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS)
  .trigger(Trigger.ProcessingTime("1 seconds"))
  .outputMode("update")
  .option("checkpointLocation", "/tmp/sparkCheckpoint/checkpoint")      // ---------checkpoint used----------
  .start()
  */