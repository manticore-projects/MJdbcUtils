# MJdbcUtils

Library supporting Named Parameters (e.g. `:Customer_Id` ) in Queries or DML/DDL statements.

[GitHub](https://github.com/manticore-projects/MJdbcUtils) [WebSite](http://manticore-projects.com/MJdbcUtils/README.html)

## Use case
When the RDBMS does not support Named Parameters directly, it will:

1) find any Named Parameter,

2) replace it with an ordinary Positional Parameter `?`,

3) maintain a map between the Position and the Named Parameter

4) provide methods for applying the Parameter Values and retrieving Parameter Type information.

It supports PreparedStatements with parameters and also rewriting/injecting SQL Statements for execution without parameters.
Rewriting/injecting is useful for Oracle Databases, which do not allow parameters for DDL Statements (not even for the query block of `CTAS`).

Furthermore, MJdbcUtils makes it easy to build an UI Parameter Dialog based on the used Parameters and the Type Information.

## Examples

Based on a Table Definition

```sql
CREATE TABLE test (
    a DECIMAL(3) PRIMARY KEY
    , b VARCHAR(128) NOT NULL
    , c DATE NOT NULL
    , d TIMESTAMP NOT NULL
    , e DECIMAL(23,5) NOT NULL
    )
```

1) We can fill the table with a simple update

    ```java
    // DML statement with Named Parameters
    String dmlStr = "INSERT INTO test VALUES ( :a, :b, :c, :d, :e )";

    // Helper function will fill our parameter map with values
    Map<String, Object> parameters = toMap("a", 1, "b", "Test String", "c", new Date(), "d", new Date(), "e", "0.12345");

    // Create a Prepared Statement, which holds our parameter mapping
    MPreparedStatement st = new MPreparedStatement(conn, dmlStr);

    // Execute our statement with the provided parameter values
    Assertions.assertFalse( st.execute(parameters) );
    ```

2) We can fill table using Batch Updates

    ```java
    int maxRecords = 100;
    int batchSize = 4;
    String dmlStr = "INSERT INTO test VALUES ( :a, :b, :c, :d, :e )";
    Map<String, Object> parameters = toMap("a", 1, "b", "Test String", "c", new Date(), "d", new Date(), "e", "0.12345");

    MPreparedStatement st = new MPreparedStatement(conn, dmlStr, batchSize);

    for (int i=0; i < maxRecords; i++) {
        parameters.put("a", i);
        parameters.put("b", "Test String " + i);

        // submit a new set of parameter values and execute automatically after 4 records
        int[] results = st.addAndExecuteBatch(parameters);
    }
    // submit any outstanding records
    st.executeBatch();

    ```

3) We can query our table

    ```java
    String qryStr = "SELECT Count(*) FROM test WHERE a = :a or b = :b";
    Map<String, Object> parameters = toMap("a", 1, "b", "Test String", "c", new Date(), "d", new Date(), "e", "0.12345");
    MPreparedStatement st = new MPreparedStatement(conn, qryStr);
     ResultSet rs = st.executeQuery(parameters);
    ```

4) We can rewrite our statement and inject the parameter values directly (useful for Oracle DDLs)

    ```java
    Date dateParameterValue = new Date();

    HashMap<String, Object> parameters = new HashMap<>();
    parameters.put("param1", "Test String");
    parameters.put("param2", 2);
    parameters.put("param3", dateParameterValue);

    String sqlStr = "select :param1, :param2, :param3;";
    String rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);

    Assertions.assertEquals("SELECT 'Test String', 2, " + getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);

    sqlStr = "UPDATE tableName SET a = :param1, b = :param2, c = :param3;";
    rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);

    Assertions.assertEquals("UPDATE tableName SET a = 'Test String', b = 2, c = " + getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);
    ```

5) We can retrieve the information about the used parameters for building a UI Dialog

    ```java
    String qryStr = "SELECT * FROM test WHERE d = :d and c = :c and b = :b and a = :a and e = :e";
    MPreparedStatement st = new MPreparedStatement(conn, qryStr);

    List<MNamedParameter> parameters = st.getNamedParametersByAppearance();
    ```

    Output of the List:

    ```text
    INFO: Found Named Parameters:
    D	java.sql.Timestamp
    C	java.sql.Date
    B	java.lang.String
    A	java.math.BigDecimal
    E	java.math.BigDecimal
    ```
