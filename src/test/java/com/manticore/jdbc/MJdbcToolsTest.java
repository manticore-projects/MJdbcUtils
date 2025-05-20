/**
 * Copyright (C) 2025 manticore-projects Co. Ltd. <support@manticore-projects.com>
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * <p>
 * This program is free software; you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program; if
 * not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA.
 * <p>
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
package com.manticore.jdbc;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Objects;

class MJdbcToolsTest {
    @Test
    void rewriteStatementWithQuotedNamedParameters() throws Exception {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "' or 'A' <> 'B");

        String sqlStr = "select * from table_a where field_a = :param1";
        String rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);
        String correctSqlStr = "SELECT * FROM table_a WHERE field_a = ''' or ''A'' <> ''B'";

        Assertions.assertEquals(correctSqlStr, rewrittenSqlStr);
    }

    @Test
    void testBooleanIssue2() throws Exception {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("param1", Boolean.TRUE);

        String sqlStr = "select * from table_a where field_a = :param1";
        String rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);
        String correctSqlStr = "SELECT * FROM table_a WHERE field_a = TRUE";

        Assertions.assertEquals(correctSqlStr, rewrittenSqlStr);
    }

    @Test
    void rewriteStatementWithNamedParameters() throws Exception {
        Date dateParameterValue = new Date();

        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "Test String");
        parameters.put("param2", 2);
        parameters.put("param3", dateParameterValue);

        String sqlStr = "select :param1, :param2, :param3;";
        String rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);

        Assertions.assertEquals(
                "SELECT 'Test String', 2, " + MJdbcTools.getSQLDateTimeStr(dateParameterValue),
                rewrittenSqlStr);

        sqlStr = "UPDATE tableName SET a = :param1, b = :param2, c = :param3;";
        rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);

        Assertions.assertEquals("UPDATE tableName SET a = 'Test String', b = 2, c = "
                + MJdbcTools.getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);
    }

    @Test
    void testRewriteStatementWithNamedParameters() throws Exception {
        Date dateParameterValue = new Date();

        String sqlStr = "select :param1, :param2, :param3;";
        String rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr,
                "Test String", 2, dateParameterValue);

        Assertions.assertEquals(
                "SELECT 'Test String', 2, " + MJdbcTools.getSQLDateTimeStr(dateParameterValue),
                rewrittenSqlStr);

        sqlStr = "UPDATE tableName SET a = :param1, b = :param2, c = :param3;";
        rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, "Test String", 2,
                dateParameterValue);

        Assertions.assertEquals("UPDATE tableName SET a = 'Test String', b = 2, c = "
                + MJdbcTools.getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);

    }

    @Test
    void getPivotFromQuery() throws SQLException, JSQLParserException {
        String ddlStr = "CREATE TABLE rep_comprehensive_income (\n"
                + "    code_inferior        VARCHAR (40)    NULL\n"
                + "    , code               VARCHAR (40)    NULL\n"
                + "    , description        VARCHAR (255)   NULL\n"
                + "    , value_date         TIMESTAMP       NULL\n"
                + "    , amount             NUMERIC (33,5)  NULL\n"
                + "    , id_currency        VARCHAR (3)     NULL\n"
                + ")\n"
                + ";\n"
                + "CREATE INDEX rep_comprehensive_income_idx1\n"
                + "    ON rep_comprehensive_income( code_inferior, code )\n"
                + ";\n"
                + "CREATE INDEX rep_comprehensive_income_idx2\n"
                + "    ON rep_comprehensive_income( value_date )\n"
                + ";\n"
                + "\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.06.000001', 'Previous year Profit and Loss', {d '2023-12-31'}, 27000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.11.000001', 'Prepaid Acquisition Costs', {d '2023-01-01'}, 10000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.11.000001', 'Prepaid Acquisition Costs', {d '2023-01-31'}, -2076.97, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.11.000001', 'Prepaid Acquisition Costs', {d '2023-02-28'}, -1903.10, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.11.000001', 'Prepaid Acquisition Costs', {d '2023-03-31'}, -2067.89, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.11.000001', 'Prepaid Acquisition Costs', {d '2023-04-30'}, -1962.77, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.11.000001', 'Prepaid Acquisition Costs', {d '2023-05-31'}, -1989.27, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '02.11.000006', 'Pending Claims', {d '2023-03-31'}, -17000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.04.110001', '0x.0x.999999', 'Settlement Account', {d '2023-01-01'}, -10000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.06.000001', 'Previous year Profit and Loss', {d '2023-12-31'}, -40000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000002', 'Expected Claims', {d '2023-01-01'}, 10000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000002', 'Expected Claims', {d '2023-03-31'}, 17000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000004', 'Contractual Service Margin', {d '2023-01-31'}, 2267.71, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000004', 'Contractual Service Margin', {d '2023-02-28'}, 2923.41, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000004', 'Contractual Service Margin', {d '2023-03-31'}, 1328.32, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000004', 'Contractual Service Margin', {d '2023-04-30'}, 1599.61, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000004', 'Contractual Service Margin', {d '2023-05-31'}, 4880.95, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000008', 'Deferred Acquisition Cost', {d '2023-01-01'}, -10000.00, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000008', 'Deferred Acquisition Cost', {d '2023-01-31'}, 1916.61, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000008', 'Deferred Acquisition Cost', {d '2023-02-28'}, 1825.65, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000008', 'Deferred Acquisition Cost', {d '2023-03-31'}, 2063.57, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000008', 'Deferred Acquisition Cost', {d '2023-04-30'}, 2040.25, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.000001', '02.11.000008', 'Deferred Acquisition Cost', {d '2023-05-31'}, 2153.92, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.100001', '02.11.000002', 'Expected Claims', {d '2023-03-31'}, -3310.78, 'USD');\n"
                + "INSERT INTO rep_comprehensive_income VALUES ('06.11.100001', '02.11.000002', 'Expected Claims', {d '2023-05-31'}, 3310.78, 'USD');\n";

        String expectedCSV =
                "CODE_INFERIOR;CODE;DESCRIPTION;ID_CURRENCY;1/1/23;1/31/23;2/28/23;3/31/23;4/30/23;5/31/23;12/31/23;Total;\n"
                        + "06.04.110001;02.06.000001;Previous year Profit and Loss;USD;;;;;;;27000.00000;27000.00000;\n"
                        + "06.04.110001;02.11.000001;Prepaid Acquisition Costs;USD;10000.00000;-2076.97000;-1903.10000;-2067.89000;-1962.77000;-1989.27000;;0.00000;\n"
                        + "06.04.110001;02.11.000006;Pending Claims;USD;;;;-17000.00000;;;;-17000.00000;\n"
                        + "06.04.110001;0x.0x.999999;Settlement Account;USD;-10000.00000;;;;;;;-10000.00000;\n"
                        + "06.04.110001;;Total;;0.00000;-2076.97000;-1903.10000;-19067.89000;-1962.77000;-1989.27000;27000.00000;0.00000;\n"
                        + ";;;;;;;;;;;;\n"
                        + "06.11.000001;02.06.000001;Previous year Profit and Loss;USD;;;;;;;-40000.00000;-40000.00000;\n"
                        + "06.11.000001;02.11.000002;Expected Claims;USD;10000.00000;;;17000.00000;;;;27000.00000;\n"
                        + "06.11.000001;02.11.000004;Contractual Service Margin;USD;;2267.71000;2923.41000;1328.32000;1599.61000;4880.95000;;13000.00000;\n"
                        + "06.11.000001;02.11.000008;Deferred Acquisition Cost;USD;-10000.00000;1916.61000;1825.65000;2063.57000;2040.25000;2153.92000;;0.00000;\n"
                        + "06.11.000001;;Total;;0.00000;4184.32000;4749.06000;20391.89000;3639.86000;7034.87000;-40000.00000;0.00000;\n"
                        + ";;;;;;;;;;;;\n"
                        + "06.11.100001;02.11.000002;Expected Claims;USD;;;;-3310.78000;;3310.78000;;0.00000;\n"
                        + "06.11.100001;;Total;;0;0;0;-3310.78000;0;3310.78000;0;0.00000;\n";

        StringBuilder builder = new StringBuilder();
        try (
                Connection conn =
                        DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");
                Statement st = conn.createStatement();) {
            for (net.sf.jsqlparser.statement.Statement parsed : CCJSqlParserUtil
                    .parseStatements(ddlStr)) {
                st.execute(parsed.toString());
            }

            try (ResultSet rs = st.executeQuery("SELECT * FROM rep_comprehensive_income");) {
                Object[][] data = MJdbcTools.getPivotFromQuery(
                        rs,
                        MJdbcTools.AggregateFunction.SUM,
                        "amount",
                        "value_date",
                        DateFormat.getDateInstance(DateFormat.SHORT, Locale.US),
                        true,
                        true);

                for (String columnName : (String[]) data[0]) {
                    builder.append(columnName).append(";");
                }
                builder.append("\n");

                for (Object[] rowData : (Object[][]) data[1]) {
                    for (Object value : rowData) {
                        builder.append(value != null ? value + ";" : ";");
                    }
                    builder.append("\n");
                }
            }
        }
        Assertions.assertEquals(expectedCSV, builder.toString());
    }

    @Test
    @Disabled
    void testDataCube() throws Exception {
        StringBuilder builder = new StringBuilder();
        try (
                InputStreamReader reader = new InputStreamReader(
                        Objects.requireNonNull(
                                MJdbcToolsTest.class.getResourceAsStream("/assets_over_time.sql")),
                        Charset.defaultCharset());
                BufferedReader bufferedReader = new BufferedReader(reader);) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                builder.append(line).append("\n");
            }
        }

        try (Connection conn =
                DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");
                Statement st = conn.createStatement();) {
            for (net.sf.jsqlparser.statement.Statement parsed : CCJSqlParserUtil
                    .parseStatements(builder.toString())) {
                st.execute(parsed.toString());
            }

            try (ResultSet rs = st.executeQuery("SELECT * FROM cfe.assets_over_time");) {
                Object[][] data = MJdbcTools.getPivotFromQuery(
                        rs,
                        MJdbcTools.AggregateFunction.SUM,
                        "amount",
                        "value_date",
                        DateFormat.getDateInstance(DateFormat.SHORT, Locale.US),
                        true,
                        true);

                for (String columnName : (String[]) data[0]) {
                    builder.append(columnName).append(";");
                }
                builder.append("\n");

                for (Object[] rowData : (Object[][]) data[1]) {
                    for (Object value : rowData) {
                        builder.append(value != null ? value + ";" : ";");
                    }
                    builder.append("\n");
                }
            }
        }
    }
}
