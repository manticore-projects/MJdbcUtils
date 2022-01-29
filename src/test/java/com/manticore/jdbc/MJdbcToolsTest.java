package com.manticore.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class MJdbcToolsTest {

    public static String getSQLDateStr(Date d) {
        return "{d '" +  MJdbcTools.SQL_DATE_FORMAT.format(d) + "'}";
    }

    public static String getSQLDateStr(Calendar c) {
        return getSQLDateStr(c.getTime());
    }

    public static String getSQLDateTimeStr(Date d) {
        return "{ts '" +  MJdbcTools.SQL_TIMESTAMP_FORMAT.format(d) + "'}";
    }

    public static String getSQLDateTimeStr(Timestamp ts) {
        return "{ts '" +  MJdbcTools.SQL_TIMESTAMP_FORMAT.format(ts) + "'}";
    }

    public static String getSQLDateTimeStr(Calendar c) {
        return getSQLDateTimeStr(c.getTime());
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

        Assertions.assertEquals("SELECT 'Test String', 2, " + getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);

        sqlStr = "UPDATE tableName SET a = :param1, b = :param2, c = :param3;";
        rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);

        Assertions.assertEquals("UPDATE tableName SET a = 'Test String', b = 2, c = " + getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);
    }

    @Test
    void testRewriteStatementWithNamedParameters() throws Exception {
        Date dateParameterValue = new Date();

        String sqlStr = "select :param1, :param2, :param3;";
        String rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, "Test String", 2, dateParameterValue);

        Assertions.assertEquals("SELECT 'Test String', 2, " + getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);

        sqlStr = "UPDATE tableName SET a = :param1, b = :param2, c = :param3;";
        rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, "Test String", 2, dateParameterValue);

        Assertions.assertEquals("UPDATE tableName SET a = 'Test String', b = 2, c = " + getSQLDateTimeStr(dateParameterValue), rewrittenSqlStr);

    }
}