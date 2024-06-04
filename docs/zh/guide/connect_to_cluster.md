## 连接到集群

### BalancedTimeplusDataSource

我们可以通过包含多个 Timeplus 实例地址的 JDBC URL 初始化 `BalancedTimeplusDataSource`，这样每次调用 `#getConnection` 时，
就可以获得的一个指向集群任一实例的健康连接。

目前, 我们只支持随机算法选择实例。
  
`BalancedTimeplusDataSource` 是线程安全的。

- 示例代码:

```java
DataSource singleDataSource = new BalancedTimeplusDataSource("jdbc:timeplus://127.0.0.1:8463");

DataSource dualDataSource = new BalancedTimeplusDataSource("jdbc:timeplus://127.0.0.1:8463,127.0.0.1:8463");

Connection conn1 = dualDataSource.getConnection();
conn1.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");

Connection conn2 = dualDataSource.getConnection();
conn2.createStatement().execute("DROP STREAM IF EXISTS test.insert_test");
conn2.createStatement().execute("CREATE STREAM IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog");
```
