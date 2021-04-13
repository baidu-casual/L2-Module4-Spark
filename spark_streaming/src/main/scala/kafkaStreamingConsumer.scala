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
import org.apache.spark.sql.expressions.Window

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
                .format("console")
                .trigger(Trigger.ProcessingTime("1 seconds"))
                .outputMode("update")
                .foreachBatch(streamingFunction _)
                .option("checkpointLocation","/tmp/spark/kafkaStreamingConsumer")
                .start()
                .awaitTermination()
    spark.close()
    
  }
  def streamingFunctionCSV(batchDf: DataFrame, batchId: Long): Unit = {
        println("\n\n\t\tBATCH "+batchId+"\n\n")

        batchDf.show(false)

        val df=batchDf.persist(StorageLevel.MEMORY_ONLY)
        println("Hello World")
        df.write.format("json")
                .mode("append")
                .option("sep",',')
                .json("/home/xs107-bairoy/baidu/L2-Module4-Spark/spark_streaming/output/output.json")

        /*
        val month = Window.partitionBy("timestamp")

        val agg_sal = batchDf
                .withColumn("max_salary", max("salary").over(month))
                .withColumn("min_salary", min("salary").over(month))
                

        agg_sal.select("depname", "max_salary", "min_salary")
                .dropDuplicates()
                .show(false)

        batchDf
                .groupBy("timestamp")
                .agg(avg("salary"))
                .show(false)*/
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

    val transactionDFCounts = transactionDF
              .withWatermark("timestamp", "10 minutes")
              //.groupBy(window($"timestamp", "10 minutes", "5 minutes"),$"value")
              //.count()
    

    transactionDFCounts
                .selectExpr("CAST(value AS STRING)")
                .writeStream
                .format("json")
                .trigger(Trigger.ProcessingTime("1 seconds"))
                .outputMode("update")
                .foreachBatch(streamingFunctionCSV _)
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
    sS.kafkaConsumeCSV()
    println("\n\n\t\tKafka Consumer Application Completed ...\n\n")
  }
}