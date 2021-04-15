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
import org.apache.spark.sql.functions.{col, from_json,to_json,struct}

/*
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
*/

class sparkStreamng {
  def streamingFunctionTest(batchDf: DataFrame, batchId: Long): Unit = {
        println("\n\n\t\tBATCH "+batchId+"\n\n")
        batchDf.show(false)

        batchDf.write.format("delta").mode("append").save("/mnt/delta/events")
        batchDf.write.format("delta").mode("append").saveAsTable("events")

        // import io.delta.implicits._
        // batchDf.write.mode("append").delta("/mnt/delta/events")


    }
  def kafkaConsumeTest(kafkaTopicName: String = "test-events", kafkaServer: String = "localhost:9092"): Unit = {
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
                .foreachBatch(streamingFunctionTest _)
                .option("checkpointLocation","/tmp/spark/kafkaStreamingConsumer")
                .start()
                .awaitTermination()
    spark.close()
    
  }
  def streamingFunction(batchDf: DataFrame, batchId: Long): Unit = {
      val conf = new SparkConf().setAppName("KAFKA sfsa").setMaster("local");
      val spark = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName("KAFKA sfsa")
                    .config(conf)
                    .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      import spark.implicits._


        println("\n\n\t\tBATCH "+batchId+"\n\n")
        
        batchDf.show(false)
        val schema = ArrayType(new StructType()
                      .add("id",IntegerType,false)
                      .add("name",StringType, false)
                      .add("dob_year",IntegerType, true)
                      .add("dob_month",IntegerType, true)
                      .add("gender",StringType, true)
                      .add("salary",IntegerType, true))
        val peopleDF = batchDf.select(from_json(col("value"), schema) as("value"))
                              .withColumn("person_explode",explode(col("value")))
                              .select("person_explode.*")
        peopleDF.printSchema()
        peopleDF.show(false)


        val windowSpec  = Window.partitionBy("timestamp").orderBy("timestamp")
        val rankTimestamp = batchDf.withColumn("rank",row_number.over(windowSpec))
        rankTimestamp.show(false)
        rankTimestamp.createOrReplaceTempView("rankTimestamp")
        spark.sql("SELECT * FROM rankTimestamp where rank<2").show(false)

        
        val df=batchDf.persist(StorageLevel.MEMORY_ONLY)

        println("\n\n\t\tWriting to JSON...")
        val pathJSON="/home/xs107-bairoy/baidu/L2-Module4-Spark/spark_streaming/output/json/"
        /*val df1 = df.toDF("id","name","dob_year","dob_month","gender","salary")
        df1.show(false)*/
        df.coalesce(1)
                .write
                .format("json")
                .mode("append")
                .option("sep",',')
                .option("header",false)
                .json(pathJSON)
        println("\n\n\tWrite Successful...")

        println("\n\n\t\tWriting to CSV...")
        val pathCSV="/home/xs107-bairoy/baidu/L2-Module4-Spark/spark_streaming/output/csv/"
        df.coalesce(1)
                .write
                .format("csv")
                .mode("append")
                .option("sep",',')
                .csv(pathCSV)
        println("\n\n\tWrite Successful...")

        println("\n\n\t\tWriting to parquet...")
        df.write.format("parquet")
                .mode("append")
                .option("sep",',')
                .parquet("/home/xs107-bairoy/baidu/L2-Module4-Spark/spark_streaming/output/parquet/")
        println("\n\n\tWrite Successful...")
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

    val transactionDFCounts = transactionDF
              .withWatermark("timestamp", "10 minutes")
              //.groupBy(window($"timestamp", "10 minutes"),$"value")
              //.count()
    // val transactionDFCounts1 = transactionDFCounts
    //           .groupBy(window($"timestamp", "10 minutes"))
    //           .count()
    // transactionDFCounts1.writeStream
    //                     .format("console")
    //                     .outputMode("update")
    //                     .start()
    

    transactionDFCounts
                .selectExpr("CAST(value AS STRING)", "timestamp")
                .writeStream
                .format("json")
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