#######################################
MJdbcUtils - JDBC Named Parameter Tools
#######################################

.. toctree::
   :maxdepth: 2
   :hidden:

   usage
   changelog
   javadoc


**MJdbcUtils** is a Java Library for the handling of Named Parameters (e. |_| g. ``:parameter``) in Queries or DML or DDL statements.
It either replaces any `Named Parameter`  with a `Positional Parameter` or rewrites the the `Named Parameter` with the parameter's value and provides a convenient mapping between the `Named Parameter` and the provided values.
Further, it provides helpers for Parameter Dialogs and Batch Updates with parameters.

Latest stable release: |MJDBCUTILS_STABLE_VERSION_LINK|

Development version: |MJDBCUTILS_SNAPSHOT_VERSION_LINK|

`GitHub Repository <https://github.com/manticore-projects/MJdbcUtils>`_

.. code-block:: Java
    :caption: Sample SQL Statement

    // DML statement with Named Parameters
    String dmlStr = "INSERT INTO test VALUES ( :a, :b, :c, :d, :e )";

    // Helper function will fill our parameter map with values
    Map<String, Object> parameters = toMap("a", 1, "b", "Test String", "c", new Date(), "d", new Date(), "e", "0.12345");

    // Create a Prepared Statement, which holds our parameter mapping
    MPreparedStatement st = new MPreparedStatement(conn, dmlStr);

    // Execute our statement with the provided parameter values
    Assertions.assertFalse( st.execute(parameters) );


*******************************
Features
*******************************

    * Finds `Named Parameters` and `Positional Parameters` in SQL Statements
    * Maintains a map between the `Named Parameter` and the derived `Positional Parameters` so ever `Named Parameter` needs to be set just one single time
       .. code-block:: Java
        :caption: Sample SQL Statement

        // DML statement with Named Parameters
        String dmlStr = "INSERT INTO test VALUES ( :a, :a, :a, :b, :b )";
        Map<String, Object> parameters = toMap("a", 1, "b", "Test String");
        MPreparedStatement st = new MPreparedStatement(conn, dmlStr);

    * Rewrites the statement using `Positional Parameters` ``?`` or the given values (useful for DDL statements)
    * Retrieve the information about the used parameters for building a UI Dialog
    * Support for `BatchUpdates` and `PreparedStatements` with parameters






