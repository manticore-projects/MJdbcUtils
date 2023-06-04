#######################################
MJdbcUtils - JDBC Named Parameter Tools
#######################################

.. toctree::
   :maxdepth: 2
   :hidden:

   usage
   Changelog <./changelog.md>
   Javadoc <./javadoc.rst>


.. image:: https://badgen.net/maven/v/maven-central/com.manticore-projects.jdbc/MJdbcUtils
    :alt: Maven Badge

.. image:: https://app.codacy.com/project/badge/Grade/e3295140a0b841f3be25da37ff8d4756
    :alt: Codacy Badge

.. image:: https://coveralls.io/repos/github/manticore-projects/MJdbcUtils/badge.svg?branch=main
    :alt: Coveralls Badge
    :target: https://coveralls.io/github/manticore-projects/MJdbcUtils?branch=main


.. image:: https://img.shields.io/badge/License-GPL-blue
    :alt: License Badge

.. image:: https://img.shields.io/github/release/manticore-projects/MJdbcUtils?include_prereleases=&sort=semver&color=blue
    :alt: GitGub Release Badge

.. image:: https://img.shields.io/github/issues/manticore-projects/MJdbcUtils
    :alt: GitGub Issues Badge

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
    * Rewrites the statement using `Positional Parameters` ``?`` or the given values (useful for DDL statements)
    * Retrieve the information about the used parameters for building a UI Dialog
    * Support for `BatchUpdates` and `PreparedStatements` with parameters






