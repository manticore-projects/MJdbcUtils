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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public class MJdbcTools {
    public static final SimpleDateFormat SQL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat SQL_TIMESTAMP_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    public final static java.sql.Date getSQLDate(Calendar cal) {
        return new java.sql.Date(cal.getTimeInMillis());
    }

    public final static java.sql.Date getSQLDate(Date d) {
        return new java.sql.Date(d.getTime());
    }

    public final static java.sql.Timestamp getSQLTimestamp(Calendar cal) {
        return new java.sql.Timestamp(cal.getTimeInMillis());
    }

    public final static java.sql.Timestamp getSQLTimestamp(Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    private static String getSQLHash(String sqlStr) throws JSQLParserException, NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);

        messageDigest.update(statement.toString().getBytes());
        String hashStr =  new String(messageDigest.digest());

        return hashStr;
    }

    private static String getParameterStr(Object o) {
        if (o == null) {
            return "NULL";
        } else if (o instanceof java.sql.Date) {
            return "{d '" + o + "'}";
        } else if (o instanceof java.util.Date) {
            Calendar cal = GregorianCalendar.getInstance();
            cal.setTime((java.util.Date) o);

            if (cal.get(Calendar.MILLISECOND) == 0
                    && cal.get(Calendar.SECOND) == 0
                    && cal.get(Calendar.MINUTE) == 0
                    && cal.get(Calendar.HOUR_OF_DAY) == 0
            ) {
                return "{d '" + SQL_DATE_FORMAT.format(o) + "'}";
            } else {
                return "{ts '" + SQL_TIMESTAMP_FORMAT.format(o) + "'}";
            }
        } else if (o instanceof java.util.Calendar) {
            Calendar cal = (Calendar) o;
            if (cal.get(Calendar.MILLISECOND) == 0
                    && cal.get(Calendar.SECOND) == 0
                    && cal.get(Calendar.MINUTE) == 0
                    && cal.get(Calendar.HOUR_OF_DAY) == 0
            ) {
                return "{d '" + SQL_DATE_FORMAT.format(cal.getTime()) + "'}";
            } else {
                return "{ts '" + SQL_TIMESTAMP_FORMAT.format(cal.getTime()) + "'}";
            }
        } else if (o instanceof java.sql.Timestamp) {
            return "{ts '" + o.toString() + "'}";
        } else if (o instanceof Long) {
            return ((Long) o).toString();
        } else if (o instanceof Integer) {
            return ((Integer) o).toString();
        } else if (o instanceof Short) {
            return ((Short) o).toString();
        } else if (o instanceof Byte) {
            return ((Byte) o).toString();
        } else if (o instanceof Double) {
            return o.toString();
        } else if (o instanceof Float) {
            return o.toString();
        } else if (o instanceof BigInteger) {
            return o.toString();
        } else if (o instanceof BigDecimal) {
            return ((BigDecimal) o).toPlainString();
        } else if (o instanceof String) {
            String s = (String) o;
            s = s.replace("'", "''");
            s = s.replace("&", "' || chr(38) || '");
            return "'" + s + "'";
        } else {
            return "'" + o + "'";
        }
    }

    public static String rewriteStatementWithNamedParameters(String sqlStr, Map<String, Object> parameters) throws Exception {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);
        StringBuilder builder = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            public void visit(JdbcNamedParameter parameter) {
                buffer.append(getParameterStr(parameters.get(parameter.getName())));
            }
        };

        SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, builder);
        expressionDeParser.setSelectVisitor(selectDeParser);
        expressionDeParser.setBuffer(builder);

        StatementDeParser statementDeParser = new StatementDeParser(expressionDeParser, selectDeParser, builder);
        statement.accept(statementDeParser);

        return builder.toString();
    }

    public static String rewriteStatementWithNamedParameters(String sqlStr, Object... parameters) throws Exception {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);
        StringBuilder builder = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            int i = 0;

            public void visit(JdbcParameter parameter) {
                buffer.append(getParameterStr(parameters[i]));
                i++;
            }

            public void visit(JdbcNamedParameter parameter) {
                buffer.append(getParameterStr(parameters[i]));
                i++;
            }
        };

        SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, builder);
        expressionDeParser.setSelectVisitor(selectDeParser);
        expressionDeParser.setBuffer(builder);

        StatementDeParser statementDeParser = new StatementDeParser(expressionDeParser, selectDeParser, builder);
        statement.accept(statementDeParser);

        return builder.toString();
    }
}
