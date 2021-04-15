package main


import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.streaming.Trigger

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.util.parsing.json.JSONObject
import java.io._
import java.time._


class institute
case class Department(id: String, name: String) extends institute
case class Employee(firstName: String, lastName: String, email: String, salary: Int) extends institute
case class DepartmentWithEmployees(department: Department, employees: Seq[Employee]) extends institute
case class KafkaProducer(first: String,last: String) extends institute

class spark extends institute{


  def createEmployee(firstName: String, lastName: String, email: String, salary: Int): Employee = {
    val employee = new Employee(firstName, lastName, email, salary)
    employee
  }

  def createBranch(id: String, name: String): Department = {
    val department = new Department(id, name)
    department
  }


  def assignBranch(department: Department, employees: Seq[Employee]): DepartmentWithEmployees = {
    val assign = new DepartmentWithEmployees(department, employees)
    assign
  }

  def temp1(): Unit = {
    val conf = new SparkConf().setAppName("Spark1").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")    
    val spark = SparkSession.builder().appName("Spark SQL").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    val department1 = new Department("123456", "Computer Science")
    val department2 = new Department("789012", "Mechanical Engineering")
    val department3 = new Department("345678", "Theater and Drama")
    val department4 = new Department("901234", "Indoor Recreation")
    val department = Seq(department1,department2,department3,department4)
    val dfDepartment = department.toDF()
    dfDepartment.show()

    val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)
    val employee5 = new Employee("michael", "jackson", "no-reply@neverla.nd", 80000)
    val employee = Seq(employee1,employee2,employee3,employee4,employee5)
    val dfEmployee = employee.toDF()
    val dsEmployee = dfEmployee.as[Employee]
    dfEmployee.show()
    dsEmployee.show()


    val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
    val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
    val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee5, employee4))
    val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))


    val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
    val df1 = departmentsWithEmployeesSeq1.toDF()
    df1.show()

    val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
    val df2 = departmentsWithEmployeesSeq2.toDF()
    df2.show()

    sc.stop()
    spark.close()
  }
  def temp2(): Unit = {
    val conf = new SparkConf().setAppName("Spark2").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")    
    val spark = SparkSession.builder().appName("Spark SQL").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    val data = Array("Gaurav", "Rohan", "Anuj", "Aryan", "Rahul", "Rohan", "Rohan", "Gaurav", "Karan")
    val distData = sc.parallelize(data)
    val pairs = distData.map(s => (s, 1))
    val count1 = pairs.reduceByKey((a, b) => a + b)

    println("\n\n\n\n\n")
    println("The Data is : "+distData.collect.foreach(println))

    println("\n\n\n\n\n")
    println("Word-Count : "+count1.collect.foreach(println))

    println("\n\n\n\n\n")
    val df1 = count1.toDF("Name","Name-Count")
    df1.show()
    df1.printSchema()
    df1.select("Name").show()
    //df1.select($"Name", $"age" + 1).show()
    //df1.filter($"age" > 21).show()
    //df1.groupBy("age").count().show()


    val file = sc.textFile("/home/xs107-bairoy/xenonstack/l2/module4/spark/files/test.txt")
    val words = file.flatMap(_.split(" "))
    val count2 = words.map(s => (s,1)).reduceByKey(_+_)
    
    println("\n\n\n\n\n")
    println("The Words are : "+words.collect.foreach(println))

    println("\n\n\n\n\n")
    println("Word-Count : "+count2.collect.foreach(println))

    println("\n\n\n\n\n")
    val df2 = count2.toDF("Word","Word-Count")
    df2.show()

    df2.createOrReplaceTempView("Word")
    val sqlDF2 = spark.sql("SELECT * FROM Word where `Word-Count`>1")
    sqlDF2.show()

    sc.stop()
    spark.close()
  }
  def temp3(): Unit = {
    val conf = new SparkConf().setAppName("Spark3").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val spark = SparkSession
                          .builder()
                          .appName("Spark SQL")
                          .config(conf)
                          .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    val pathCsv = "/home/xs107-bairoy/xenonstack/l2/module4/spark/files/data.csv"
    val df1 = spark.read.option("header", "true").format("csv").load(pathCsv)

    val dfPersist = df1.toDF().persist(StorageLevel.MEMORY_AND_DISK)
    dfPersist.show()

    df1.createOrReplaceTempView("Orders")
    spark.sql("SELECT * FROM Orders where status='pending'").show()
    spark.sql("SELECT * FROM Orders where total>40").show()

    val pathJson = "/home/xs107-bairoy/xenonstack/l2/module4/spark/files/data1.json"
    val df = spark.read.option("multiline", "true").json(pathJson)
    df.createOrReplaceTempView("temp")

    df.show()
    val array = spark.sql("SELECT array FROM temp")
    array.show()
    
    sc.stop()
    spark.close()
  }
  def temp4(): Unit = {
    val conf = new SparkConf().setAppName("Spark4").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    val pathJson = "/home/xs107-bairoy/xenonstack/l2/module4/spark/files/parquet/userdata1.parquet"
    val parquetFileDF = spark.read.option("multiLines","true").option("inferSchema","true").parquet(pathJson)
    val df = parquetFileDF.toDF().persist(StorageLevel.MEMORY_AND_DISK)
    //df.show()
    
    df.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile where id<=20").show()
    spark.sql("SELECT * FROM parquetFile where id>20").show()
    spark.sql("SELECT * FROM parquetFile where id>40").show()
    spark.sql("SELECT * FROM parquetFile where id>60").show()
    spark.sql("SELECT * FROM parquetFile where id>80").show()
    spark.sql("SELECT * FROM parquetFile where id>100").show()
    spark.sql("SELECT * FROM parquetFile where id>120").show()
    spark.sql("SELECT * FROM parquetFile where id>140").show()
    spark.sql("SELECT * FROM parquetFile where id>160").show()
    spark.sql("SELECT * FROM parquetFile where id>180").show()
    spark.sql("SELECT * FROM parquetFile where id>200").show()
    spark.sql("SELECT * FROM parquetFile where id>220").show()
    spark.sql("SELECT * FROM parquetFile where id>240").show()
    spark.sql("SELECT * FROM parquetFile where id>260").show()
    spark.sql("SELECT * FROM parquetFile where id>280").show()
    spark.sql("SELECT * FROM parquetFile where id>300").show()
    spark.sql("SELECT * FROM parquetFile where id>320").show()
    spark.sql("SELECT * FROM parquetFile where id>340").show()
    spark.sql("SELECT * FROM parquetFile where id>360").show()
    spark.sql("SELECT * FROM parquetFile where id>380").show()
    spark.sql("SELECT * FROM parquetFile where id>400").show()
    spark.sql("SELECT * FROM parquetFile where id>420").show()
    spark.sql("SELECT * FROM parquetFile where id>440").show()
    spark.sql("SELECT * FROM parquetFile where id>460").show()
    spark.sql("SELECT * FROM parquetFile where id>480").show()
    spark.sql("SELECT * FROM parquetFile where id>500").show()
    spark.sql("SELECT * FROM parquetFile where id>520").show()
    spark.sql("SELECT * FROM parquetFile where id>540").show()
    spark.sql("SELECT * FROM parquetFile where id>560").show()
    spark.sql("SELECT * FROM parquetFile where id>580").show()
    spark.sql("SELECT * FROM parquetFile where id>600").show()
    spark.sql("SELECT * FROM parquetFile where id>620").show()
    spark.sql("SELECT * FROM parquetFile where id>640").show()
    spark.sql("SELECT * FROM parquetFile where id>660").show()
    spark.sql("SELECT * FROM parquetFile where id>680").show()
    spark.sql("SELECT * FROM parquetFile where id>700").show()
    spark.sql("SELECT * FROM parquetFile where id>720").show()
    spark.sql("SELECT * FROM parquetFile where id>740").show()
    spark.sql("SELECT * FROM parquetFile where id>760").show()
    spark.sql("SELECT * FROM parquetFile where id>780").show()
    spark.sql("SELECT * FROM parquetFile where id>800").show()
    spark.sql("SELECT * FROM parquetFile where id>920").show()
    spark.sql("SELECT * FROM parquetFile where id>940").show()
    spark.sql("SELECT * FROM parquetFile where id>960").show()
    spark.sql("SELECT * FROM parquetFile where id>980").show()
    spark.sql("SELECT * FROM parquetFile where id>1000").show()

    /**
      * Un-Comment the lines below to save parquet to csv files
      */   
    //val path = "/home/xs107-bairoy/xenonstack/l2/module4/spark/output/userdata1.csv"
    //df.coalesce(1).write.csv(path)
    
    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost:9092")
      .option("port", 9999)
      .load("/home/baidu/xenonstack/l2/module4/spark/files/temp.txt")
    /*
    socketDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .outputMode("append")
      .start()
      .awaitTermination()

    val df2 = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .load()

    val df1 = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "topic1")
          .load()
    df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .as[(String, String)]*/
    val csvDF = spark
          .readStream
          .option("sep", ";")     // Specify schema of the csv files
          .csv("/home/xs107-bairoy/xenonstack/l2/module4/spark/files/csv/userdata1.csv")
    

    csvDF.show()

    sc.stop()
    spark.close()
  }
  def temp5(): Unit = {
    println("Spark Structured Streaming with Kafka Demo Application Started ...")

    val KAFKA_TOPIC_NAME_CONS = "testtopic"
    val KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "34.73.102.250:9092"

    System.setProperty("HADOOP_USER_NAME","hadoop")

    val spark = SparkSession.builder
        .master("local[*]")
        .appName("Spark Structured Streaming with Kafka Demo")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Stream from Kafka
    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    val transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    // Define a schema for the transaction_detail data
    val transaction_detail_schema = StructType(Array(
      StructField("transaction_id", StringType),
      StructField("transaction_card_type", StringType),
      StructField("transaction_amount", StringType),
      StructField("transaction_datetime", StringType)
    ))

    val transaction_detail_df2 = transaction_detail_df1
      .select(from_json(col("value"), transaction_detail_schema)
        .as("transaction_detail"), col("timestamp"))

    val transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

    // Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    val transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type")
    .agg(sum(col("transaction_amount")).as("total_transaction_amount"))

    println("Printing Schema of transaction_detail_df4: ")
    transaction_detail_df4.printSchema()

    val transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100))
      .withColumn("value", concat(lit("{'transaction_card_type': '"),
        col("transaction_card_type"), lit("', 'total_transaction_amount: '"),
        col("total_transaction_amount").cast("string"), lit("'}")))

    println("Printing Schema of transaction_detail_df5: ")
    transaction_detail_df5.printSchema()

    // Write final result into console for debugging purpose
    val trans_detail_write_stream = transaction_detail_df5
      .writeStream
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .outputMode("update")
      .option("truncate","false")
      .format("console")
      .start()

    // Write final result in the Kafka topic as key, value
    val trans_detail_write_stream_1 = transaction_detail_df5
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS)
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .outputMode("update")
      .option("checkpointLocation", "file:///D://work//development//spark_structured_streaming_kafka//checkpoint")
      .start()

    trans_detail_write_stream.awaitTermination()

    println("Spark Structured Streaming with Kafka Demo Application Completed.")
  }
  def broadcastJoins(): Unit = {
    val conf = new SparkConf().setAppName("Spark4").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    //val ssc = new StreamingContext(sc, Seconds(1))
    import spark.implicits._


    
    val peopleDF = Seq(
                  ("andrea", "medellin"),
                  ("rodolfo", "medellin"),
                  ("abdul", "bangalore")).toDF("first_name", "city")
    peopleDF.show()
    val citiesDF = Seq(
                  ("medellin", "colombia", 2.5),
                  ("bangalore", "india", 12.3)).toDF("city", "country", "population")
    citiesDF.show()

    peopleDF.join(broadcast(citiesDF),
                  peopleDF("city") <=> citiesDF("city")).show()

    
    val trainsCsv = "/home/xs107-bairoy/xenonstack/l2/module4/spark/files/trains/trains.csv"
    val trains = spark.read.option("header", "true").format("csv").load(trainsCsv).toDF()

    val carsCsv = "/home/xs107-bairoy/xenonstack/l2/module4/spark/files/trains/cars.csv"
    val cars = spark.read.option("header", "true").format("csv").load(carsCsv).toDF()

    trains.show()
    cars.show()

    trains.createOrReplaceTempView("trains")
    cars.createOrReplaceTempView("cars")

    val trainsDF=spark.sql("SELECT * from trains")
    val carsDF=spark.sql("SELECT * from cars")
    carsDF.join(broadcast(trainsDF),
                  carsDF("id") <=> trainsDF("id")).show()


    sc.stop()
    spark.close()
  }
  def temp6(): Unit = {
    val filename = Random.nextString(2)+".json"
    val path="/home/xs107-bairoy/xenonstack/l2/module4/spark_streaming_producer/files/person/"+filename
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    // var json="[\n"
    var json=""
    for ( a <- 1 to 200){
    val temp = Map(
      "id" -> Random.nextInt(10000),
      "name" -> Random.nextString(10),
      "dob_year" -> Random.nextInt(3000),
      "dob_month" -> Random.nextInt(12),
      "gender" -> Random.nextString(5),
      "salary" -> Random.nextInt(9999999)
    )
    var jsonBoolean = JSONObject(temp).toString()
    // if (a<=99){
    //   jsonBoolean+=",\n"
    // }    
    json+=(jsonBoolean+"\n")
    }
    // json+="\n]"
    bw.write(json)
    println(json)
    bw.close()    
  }
  def saveDfToCsv(df: DataFrame, name: String/*, sep: String = ",", header: Boolean = false*/): Unit = {
    
    val path = "/home/xs107-bairoy/xenonstack/l2/module4/spark/output"

    df.repartition(1)
      .write
      .format(name)
      .save(path)
    /*df.repartition(1).write.
        format("com.databricks.spark.csv").
        option("header", header.toString).
        option("delimiter", sep).
        save("Path")

    val dir = new File(tmpParquetDir)
    val newFileRgex = tmpParquetDir + File.separatorChar + ".part-00000.*.csv"
    val tmpTsfFile = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString
    (new File(tmpTsvFile)).renameTo(new File(tsvOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete*/
  }
}


//
object demo extends institute {  
  def main(args: Array[String]): Unit = {
    val institute1 = new spark
    //institute1.temp1()
    //institute1.temp2()
    //institute1.temp3()
    //institute1.temp4()
    //institute1.temp5()
    //institute1.broadcastJoins()
    for(a <- 1 to 20){
      institute1.temp6()
      Thread.sleep(9000)
    }
  }
}
/**
*Dear Sir, I'm 
*/
