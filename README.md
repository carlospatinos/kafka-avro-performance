# kafka-avro-performance
Test app publishing and consuming kafka messages with avro in Java

![BUILD2](https://github.com/github/docs/actions/workflows/gradle-test.yml/badge.svg)
![BUILD](https://github.com/carlospatinos/kafka-avro-performance/actions/workflows/gradle-test.yml/badge.svg)



# Requirements 
- Java 8 installed
- Gradle (alternatively you can use the wrapper here gradlew)

# Run
Make sure the project compiles.

```sh
gradle build
```

A docker-compose file is inside kafka-docker which will pull images for kafka, zookeeper and schema registry. This needs to be up and running before the actual testing to make sure infrastructure is in place.

```sh
cd kafka-docker

docker-compose up
```
