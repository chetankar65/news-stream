## Newsy: A simple project to understand the basics of Kafka. 

Publishing of live news feed into kafka and streaming it via a flask dashboard.

## Commands to run Kafka after installing on WSL:

1. Run ZooKeeper
    - bin/zookeeper-server-start.sh config/zookeeper.properties

2. Run Kafka
    - bin/kafka-server-start.sh config/server.properties

3. Run Maven build tool
    - Build: mvn clean package
    - Run: mvn exec:java -Dexec.mainClass=com.newsy.FinancialNewsProducer

4. Run the python dashboard
    - source venv/bin/activate
    - python3 app.py


