# L2-Module4-Spark
#### This repository contains 3 scala projects:
- _spark_
- _spark_streaming_
- _spark_streaming_producer_
## spark
#### Features
- _class institute_ has 3 case classes defined for use in class spark
- _class spark_ has following functions implemented
    - _temp1()_ - case classes to spark dataframe
    - _temp2()_ - WordCount operations in Spark RDD using scala arrays and text files
    - _temp3() & temp4()_ - Spark Dataframes and DataSets, performed basic sparksql operations, and Spark Persistence
    - _broadcastJoins()_ - Spark Jobs for implementing broadcast joins
    - _randomJsonGenerator()_ 
        - Generates Random JSON files in path : "L2-Module4-Spark/spark_streaming_producer/files/person" for the kafka Producer to read from and write to kafka topic.
        - You need to change the path variables in this function for usage.
- _object demo_ has in the entry point in scala file "demo.scala" through def main.

## spark_streaming_producer
#### Features
- _class sparkStreamng_ has following functions implemented
    - _kafkaProduce(kafkaTopicName: String = "test-events", kafkaServer: String = "localhost:9092")_
        - Takes a Kafka Topic name and Kafka Server as input
        - Reads json files from "L2-Module4-Spark/spark_streaming_producer/files/person" using _df.readStream()_ function and writes, using _df.writeStream()_ function, to Kafka Topic at Kafka Server specified in function parameters.
    - _streamingFunctionTest(batchDf: DataFrame, batchId: Long)_
        - Prints each batch data sent to Kafka Topic for testing purposes.
- _object kafkaStreamingProducer_ has in the entry point in scala file "kafkaStreamingProducer.scala" through def main.

## spark_streaming
#### Features
- _class Consumer_ has following functions implemented
    - _kafkaConsume(kafkaTopicName: String = "test-events", kafkaServer: String = "localhost:9092")_
        - Takes a Kafka Topic name and Kafka Server as input
        - Reads data, using _df.readStream()_ function, from Kafka Topic at Kafka Server specified in function parameters and writes the data as _json_ format in batches to the _streamingFunction(batchDf: DataFrame, batchId: Long)_
    - _streamingFunction(batchDf: DataFrame, batchId: Long)_
        - Prints each batch data sent from Kafka Topic.
    - _object kafkaStreamingConsumer_ has in the entry point in scala file "kafkaStreamingConsumer.scala" through def main.
>_class Consumer_ is still in development.
>    - Following functions are either for testing purposes or in development:
>        - streamingFunctionTest(batchDf: DataFrame, batchId: Long)
>        - kafkaConsumeTest(kafkaTopicName: String = "test-events", kafkaServer: String = "localhost:9092")
>        - deltaFunction(deltaDf: DataFrame)
>        - streamingFunction(batchDf: DataFrame, batchId: Long)