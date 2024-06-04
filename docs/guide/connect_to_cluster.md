## Connect to Cluster

### BalancedTimeplusDataSource

We can initial `BalancedTimeplusDataSource` with a jdbc url which contains multiple Timeplusd instance addresses, 
and each time when call `#getConnection`, a health connection which connected to one of the instances will be given. 

Currently, we only support random algorithm for timeplusd instances selection.
  
The `BalancedTimeplusDataSource` can be shared in different threads.

- Example codes:

```java
DataSource singleDataSource = new BalancedTimeplusDataSource("jdbc:timeplus://127.0.0.1:8463");

DataSource dualDataSource = new BalancedTimeplusDataSource("jdbc:timeplus://127.0.0.1:8463,127.0.0.1:8464");

Connection conn1 = dualDataSource.getConnection();
conn1.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");

Connection conn2 = dualDataSource.getConnection();
conn2.createStatement().execute("DROP STREAM IF EXISTS test.insert_test");
conn2.createStatement().execute("CREATE STREAM IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog");
```
