package main


import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.streaming._

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, from_json,to_json,struct}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


import org.apache.spark.sql.streaming.Trigger

import java.nio.file.{Files, Paths}



class sparkStreamng{
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
    .master("local[4]")
    .appName("Spark Kafka Producer")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    System.setProperty("HADOOP_USER_NAME","hadoop") 
    
    
    val schema = new StructType()
                                .add("id",IntegerType,false)
                                .add("name",StringType, false)
                                .add("dob_year",IntegerType, true)
                                .add("dob_month",IntegerType, true)
                                .add("gender",StringType, true)
                                .add("salary",IntegerType, true)
    val schema1 =ArrayType(schema)
      
      /*id":1,
      "name":"James Smith",
      "dob_year":2018,
      "dob_month":1,
      "gender":"M",
      "salary":3000*/
      
    val personJson = "/home/xs107-bairoy/xenonstack/l2/module4/spark_streaming_producer/files/person"
    val people = spark.readStream
                .schema(schema)
                .json(personJson)
    people.printSchema()
    //people.createOrReplaceTempView("value")
    //val temp=spark.sql("select * from value")
    
    val peopleDF = people.selectExpr("struct(*) AS value")
      //.select(from_json(col("value"), schema))
      //.withColumn("person_explode",explode(col("value")))
      //.select("value.*")
    peopleDF.printSchema()

    peopleDF
          .selectExpr("CAST(value AS STRING)")          
          .writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaServer)
          .option("topic", kafkaTopicName)
          //.trigger(Trigger.ProcessingTime("30 seconds"))
          .outputMode("append")
          //.foreachBatch(streamingFunction _)
          .option("checkpointLocation","/tmp/spark")
          .start()
          .awaitTermination()


      spark.close()
    
  }
  /*
  def temp(kafkaTopicName: String = "test", kafkaServer: String = "localhost:9092"): Unit = {

    val conf = new SparkConf().setAppName("KAFKA").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
    .builder()
    .master("local[4]")
    .appName("Spark Kafka Producer")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    System.setProperty("HADOOP_USER_NAME","hadoop")


    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", kafkaTopicName)
      .option("startingOffsets", "earliest")
      .load()    
    df.printSchema()


    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    

    /*
      
    personDF.select(to_json(struct("data.*")) as "value")
      .writeStream
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", kafkaTopicName)
      .option("checkpointLocation","/tmp/spark/kafkaStreamingProducer")
      .start()*/
      

      spark.close()
  }*/
  
}
object kafkaStreamingProducer {  
  def main(args: Array[String]): Unit = {
    println("\n\n\t\tKafka Producer Application Started ...\n\n")
    val sS = new sparkStreamng
    sS.kafkaConsume()
    //sS.temp()
    println("\n\n\t\tKafka Producer Application Completed ...\n\n")
  }  
}