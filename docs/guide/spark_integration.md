## Integration with Spark

### Requirements

- Java 8, Scala 2.11/2.12, Spark 2.4
- Or Java 8/11, Scala 2.12, Spark 3.0/3.1

For Spark 3.2, [Spark Timeplus Connector](https://github.com/timeplus-io/spark-timeplus-connector) is recommended.

**Notes:** Spark 2.3.x(EOL) should also work fine. Actually we do test on both Java 8 and Java 11, 
but Spark official support on Java 11 since 3.0.0.

### Import

- Gradle

```groovy
// available since 2.4.0
compile "com.timeplus:timeplus-integration-spark_2.11:${timeplus_native_jdbc_version}"
```

- Maven

```xml
<!-- available since 2.4.0 -->
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>timeplus-integration-spark_2.11</artifactId>
    <version>${timeplus-native-jdbc.version}</version>
</dependency>
```

### Examples

Make sure register `TimeplusDialect` before using it

```scala
    JdbcDialects.registerDialect(TimeplusDialect)
```

Read from Timeplus to DataFrame

```scala
val df = spark.read
    .format("jdbc")
    .option("driver", "com.timeplus.jdbc.TimeplusDriver")
    .option("url", "jdbc:timeplus://127.0.0.1:8463")
    .option("user", "default")
    .option("password", "")
    .option("dbtable", "db.test_source")
    .load
```

Write DataFrame to Timeplus (support `truncate table`)

```scala
df.write
    .format("jdbc")
    .mode("overwrite")
    .option("driver", "com.timeplus.jdbc.TimeplusDriver")
    .option("url", "jdbc:timeplus://127.0.0.1:8463")
    .option("user", "default")
    .option("password", "")
    .option("dbtable", "db.test_target")
    .option("truncate", "true")
    .option("batchsize", 10000)
    .option("isolationLevel", "NONE")
    .save
```

See also [SparkOnTimeplusITest](https://github.com/timeplus-io/timeplus-native-jdbc/timeplus-integration/timeplus-integration-spark/src/test/scala/com.timeplus.jdbc.spark/SparkOnTimeplusITest.scala)
