/**
 * Copyright (C) 2021 Andreas Reichel <andreas@manticore-projects.com>
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
package com.manticore.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

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
    void rewriteStatementWithQuotedNamedParameters() throws Exception {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "' or 'A' <> 'B");

        String sqlStr = "select * from table_a where field_a = :param1";
        String rewrittenSqlStr = MJdbcTools.rewriteStatementWithNamedParameters(sqlStr, parameters);
        String correctSqlStr = "SELECT * FROM table_a WHERE field_a = ''' or ''A'' <> ''B'";

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