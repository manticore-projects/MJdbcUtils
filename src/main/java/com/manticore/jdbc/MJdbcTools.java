/**
 * Copyright (C) 2023 manticore-projects Co. Ltd. <support@manticore-projects.com>
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
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public class MJdbcTools {
    public static final DateTimeFormatter SQL_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static java.sql.Date getSQLDate(Calendar cal) {
        return new java.sql.Date(cal.getTimeInMillis());
    }

    public static java.sql.Date getSQLDate(Date d) {
        return new java.sql.Date(d.getTime());
    }

    public static java.sql.Timestamp getSQLTimestamp(Calendar cal) {
        return new java.sql.Timestamp(cal.getTimeInMillis());
    }

    public static java.sql.Timestamp getSQLTimestamp(Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    public static String getSQLDateStr(Date d) {
        LocalDate localDate = Instant
                .ofEpochMilli(d.getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        return "{d '" + MJdbcTools.SQL_DATE_FORMAT.format(localDate) + "'}";
    }

    public static String getSQLDateStr(Calendar c) {
        return getSQLDateStr(c.getTime());
    }

    public static String getSQLDateTimeStr(Date d) {
        LocalDateTime localDateTime = d.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        return "{ts '" + MJdbcTools.SQL_TIMESTAMP_FORMAT.format(localDateTime) + "'}";
    }

    public static String getSQLDateTimeStr(java.sql.Timestamp ts) {
        LocalDateTime localDateTime = ts.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        return "{ts '" + MJdbcTools.SQL_TIMESTAMP_FORMAT.format(localDateTime) + "'}";
    }

    public static String getSQLDateTimeStr(Calendar c) {
        return getSQLDateTimeStr(c.getTime());
    }

    public static String getSQLHash(String sqlStr)
            throws JSQLParserException, NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);

        messageDigest.update(statement.toString().getBytes(Charset.defaultCharset()));
        String hashStr = new String(messageDigest.digest(), Charset.defaultCharset());

        return hashStr;
    }

    private static String getParameterStr(Object o) {
        if (o == null) {
            return "NULL";
        } else if (o instanceof java.sql.Date) {
            return "{d '" + o + "'}";
        } else if (o instanceof Date) {
            Calendar cal = GregorianCalendar.getInstance();
            cal.setTime((Date) o);

            if (cal.get(Calendar.MILLISECOND) == 0
                    && cal.get(Calendar.SECOND) == 0
                    && cal.get(Calendar.MINUTE) == 0
                    && cal.get(Calendar.HOUR_OF_DAY) == 0) {
                LocalDate localDate = Instant
                        .ofEpochMilli(((Date) o).getTime())
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate();

                return "{d '" + SQL_DATE_FORMAT.format(localDate) + "'}";
            } else {
                LocalDateTime localDateTime = ((Date) o).toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
                return "{ts '" + SQL_TIMESTAMP_FORMAT.format(localDateTime) + "'}";
            }
        } else if (o instanceof Calendar) {
            Calendar cal = (Calendar) o;
            if (cal.get(Calendar.MILLISECOND) == 0
                    && cal.get(Calendar.SECOND) == 0
                    && cal.get(Calendar.MINUTE) == 0
                    && cal.get(Calendar.HOUR_OF_DAY) == 0) {
                LocalDate localDate = Instant
                        .ofEpochMilli(cal.getTimeInMillis())
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate();

                return "{d '" + SQL_DATE_FORMAT.format(localDate) + "'}";
            } else {
                LocalDateTime localDateTime = cal.toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
                return "{ts '" + SQL_TIMESTAMP_FORMAT.format(localDateTime) + "'}";
            }
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

    public static String rewriteStatementWithNamedParameters(String sqlStr,
            Map<String, Object> parameters) throws Exception {
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

        StatementDeParser statementDeParser =
                new StatementDeParser(expressionDeParser, selectDeParser, builder);
        statement.accept(statementDeParser);

        return builder.toString();
    }

    public static String rewriteStatementWithNamedParameters(String sqlStr, Object... parameters)
            throws Exception {
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

        StatementDeParser statementDeParser =
                new StatementDeParser(expressionDeParser, selectDeParser, builder);
        statement.accept(statementDeParser);

        return builder.toString();
    }
}
