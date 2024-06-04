Timeplus Native JDBC
======================
这是一个用原生(TCP)协议实现的 JDBC 驱动，用来访问 [Timeplus](https://timeplus.com/) ，同时也支持与 [Apache Spark](https://github.com/apache/spark/) 的集成。

本项目受 [JetBrains Open Source License](https://www.jetbrains.com/?from=timeplus-native-jdbc) 2020-2021 赞助支持. 

## JDBC 驱动

### 与 Timeplus [proton-jdbc-driver](https://github.com/timeplus-io/proton-jdbc-driver) 驱动的不同点

* 写入时，数据按照列式格式组织并压缩
* 基于 TCP 协议实现，比 HTTP 协议更高效，参考 [性能测试报告](docs/dev/benchmark.md)。

### 限制

* 不支持复杂表达式语句的批量写入，如：`INSERT INTO test_table VALUES(toDate(123456))`，但查询不受影响。
* 不支持写入 non-values 格式，如 `TSV`。
* 不支持 `ZSTD` 压缩。

## Spark 集成

目前的实现基于 Spark JDBC API，支持数据类型映射，自动建表，表清空(truncate)，表读写等。

## 开源协议

Apache License (Version 2.0)。详情参考 [LICENSE](https://github.com/timeplus-io/timeplus-native-jdbc/LICENSE).
