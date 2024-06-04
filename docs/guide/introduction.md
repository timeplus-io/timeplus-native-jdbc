Timelus Native JDBC
======================
A Native JDBC library for accessing [Timeplus](https://timeplus.com/) in Java, also provide a library for 
integrating with [Apache Spark](https://github.com/apache/spark/).

Supported by [JetBrains Open Source License](https://www.jetbrains.com/?from=timeplus-native-jdbc) 2020-2021. 

## JDBC Driver

### Differences from Timeplus [proton-jdbc-driver](https://github.com/timeplus-io/proton-jdbc-driver)

* Data is organized and compressed by columns.
* Implemented in the TCP Protocol, with higher performance than HTTP, here is the [benchmark report](../dev/benchmark.md).

### Limitations

* Not support insert complex values expression, like `INSERT INTO test_table VALUES(to_date(123456))`, but query is ok.
* Not support insert non-values format, like `TSV`.
* Not support more compression method, like `ZSTD`.

## Spark Integration

Currently，the implementation based on Spark JDBC API, support data type mapping, auto create table, truncate table, write, read, etc.

## License

This project is distributed under the terms of the Apache License (Version 2.0). See [LICENSE](https://github.com/timeplus-io/timeplus-native-jdbc/LICENSE) for details.
