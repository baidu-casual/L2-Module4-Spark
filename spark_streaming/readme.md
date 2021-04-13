<H1>DEPLOY:</H1><br>
1. Start ZooKeeper Service          : <br><br>zookeeper-server-start.sh config/zookeeper.properties<br><br>
2. Start Broker Service             : <br><br>kafka-server-start.sh config/server.properties<br><br>
3. Create a kafka topic             : <br><br>kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092<br><br>
4. Describe Topic                   : <br><br>kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092<br><br>
5. Run the console producer client  : <br><br>kafka-console-producer.sh --topic testtopic --bootstrap-server localhost:9092<br><br>
6. Run the script to deploy the Spark Kafka Application : <br>./run.sh<br><br><br>
<H1>NOTE:</H1><br>Also push changes to GITHUB : <br><br>./git.sh