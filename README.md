Timeplus Native JDBC
===

A Native JDBC library for accessing [Timeplus](https://timeplus.com/) in Java with streaming SQL support.

This is a fork of https://github.com/housepower/ClickHouse-Native-JDBC, but revised and enhanced for streaming query processing. If you don't need to run streaming SQL or has lower performance need, you may also use https://github.com/timeplus-io/proton-java-driver.

## CONTRIBUTE

We welcome anyone that wants to help out in any way, whether that includes reporting problems, helping with documentations, or contributing code changes to fix bugs, add tests, or implement new features. Please follow [Contributing Guide](CONTRIBUTE.md).

## JDBC Driver

### Requirements

- Java 8/11. 

**Notes:** We only do test with Java LTS versions.

* Data is organized and compressed by columns.
* Implemented in the TCP Protocol, with higher performance than HTTP, here is the [benchmark report](docs/dev/benchmark.md).

### Limitations

* Not support insert complex values expression, like `INSERT INTO test_table VALUES(to_date(123456))`, but query is ok.
* Not support insert non-values format, like `TSV`.
* Not support more compression method, like `ZSTD`.

### Import

- Gradle
```groovy
compile "com.timeplus:timeplus-native-jdbc:${timeplus_native_jdbc_version}"
```
Or use the shade version with dependencies packed in the same JAR.
```groovy
compile "com.timeplus:timeplus-native-jdbc-shaded:${timeplus_native_jdbc_version}"
```

- Maven

```xml
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>timeplus-native-jdbc</artifactId>
    <version>${timeplus-native-jdbc.version}</version>
</dependency>
```
Or use the shade version with dependencies packed in the same JAR.
```xml
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>timeplus-native-jdbc-shaded</artifactId>
    <version>${timeplus-native-jdbc.version}</version>
</dependency>
```

### Example Code

Unlike [the Proton JDBC Driver](https://github.com/timeplus-io/proton-java-driver) which only supports batch query, this JDBC driver talks to the native port of Timeplus and support streaming SQL. The query will be long running and you can get latest results from the JDBC ResultSet.

Here is an example to run a DDL to create a random stream, then run a streaming SQL to list all events.
```java
package test_jdbc_driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class App {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:timeplus://localhost:8463";
        // to connect Timeplus Enterprise with username and password: jdbc:timeplus://localhost:8463?user=admin&password=changeme

        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeQuery(
                    "create random stream if not exists simple_random(i int, s string) settings eps=3"
                );
                try (
                    ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM simple_random"
                    )
                ) {
                    while (rs.next()) {
                        System.out.println(
                            rs.getInt(1) + "\t" + rs.getString(2)
                        );
                    }
                }
            }
        }
    }
}


```
## License

This project is distributed under the terms of the Apache License (Version 2.0). See [LICENSE](LICENSE) for details.
