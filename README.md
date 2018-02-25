# Kafka Stream Processor

This is a stream processor app that connects to Kafka to perform joins and transformations to the post, subscriber and subscription streams used in this demo.

## Prerequisites

#### Offline
* Java 8
* Maven 3.1+

## Setting up the Stream Processor
* Clone the git repository
* Go to the project directory and perform ‘mvn clean package‘

## Starting the Stream Processor
* Run ‘java -jar target/kafka-stream-processor-0.1.0-SNAPSHOT-jar-with-dependencies.jar‘

## Using Docker Containers to run the application
* Make sure Docker is available on your machine
* Run ‘mvn docker:build‘
* Run ‘docker-compose up -d kafka-stream-processor‘ 
