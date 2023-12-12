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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.Format;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

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

    public enum AggregateFunction {
        SUM
        , COUNT
    }

    private static TreeMap<Object, Object> emptyMapFromKeys(TreeSet<Object> keys) {
        TreeMap<Object, Object> map = new TreeMap<>();
        for (Object k:keys) {
            map.put(k, null);
        }
        return map;
    }

    private static class ObjectArrayKey implements Comparable<ObjectArrayKey> {
        private final Comparable[] keys;

        public ObjectArrayKey(Object[] keys) {
            this.keys = new Comparable[keys.length];
            for (int i=0; i<keys.length; i++) {
                this.keys[i] = (Comparable<?>) keys[i];
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ObjectArrayKey that = (ObjectArrayKey) o;

            return Arrays.deepEquals(keys, that.keys);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }

        @Override
        public int compareTo(ObjectArrayKey other) {
            // Assuming both arrays have the same length
            for (int i = 0; i < keys.length; i++) {
                int elementComparison = keys[i].compareTo(other.keys[i]);
                if (elementComparison != 0) {
                    return elementComparison;
                }
            }

            // If all elements are equal, the arrays are considered equal
            return 0;
        }
    }

    /**
     * The getPivotFromQuery function takes a ResultSet and converts it into a pivot table.
     * The function is designed to create columns for each key of the Category Column and to aggregate the values
     * of the Aggregate Column for each Category.
     *
     * @param rs The ResultSet holding the source data with the category values in rows
     * @param function Determine what type of aggregate function to use (SUM, COUNT, ...)
     * @param categoryColumnName Identify the column that will be transformed into separate Value Columns
     * @param categoryFormat Format the key values into column labels
     * @param aggregateColumnName Specify the column name of the aggregate value
     *
     * @return A 2-dimensional array holding the Column Names and the Data
     *
     */
    public static Object[][] getPivotFromQuery(ResultSet rs, AggregateFunction function, String categoryColumnName, String aggregateColumnName, Format categoryFormat) throws SQLException {
        ArrayList<String> columnNames = new ArrayList<>();
        int categoryColumnIndex = -1;
        Class<?> categoryClass = null;
        int aggregateColumnIndex = -1;
        Class<?> aggregateClass = null;

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i=1; i<=columnCount; i++) {
            String columnName =  metaData.getColumnName(i);
            if (columnName.equalsIgnoreCase(categoryColumnName)) {
                try {
                    categoryColumnIndex = i;
                    categoryClass = Class.forName( metaData.getColumnClassName(i));
                } catch (ClassNotFoundException ignore) {
                }
            } else if (columnName.equalsIgnoreCase(aggregateColumnName)) {
                try {
                    aggregateColumnIndex = i;
                    aggregateClass = Class.forName( metaData.getColumnClassName(i));
                } catch (ClassNotFoundException ignore) {
                }
            } else {
                columnNames.add( columnName );
            }
        }

        if (categoryClass==null || aggregateClass==null) {
            throw new RuntimeException("Unable to determine the Aggregate or Category Classes.");
        }


        TreeSet<Object> keys = new TreeSet<>();
        TreeMap<ObjectArrayKey, TreeMap<Object, Object>> data = new TreeMap<>();
        ArrayList<Object> columnValues = new ArrayList<>();
        Object keyValue;
        Object aggregateValue;
        while (rs.next()) {
            columnValues.clear();
            keyValue = null;
            aggregateValue = null;

            for (int i = 1; i<=columnCount; i++) {
                Object value = rs.getObject(i);

                if (i==categoryColumnIndex) {
                    keyValue = value;
                    // if we meet the key first time, we need to register it and extend the aggregated data maps
                    if (!keys.contains(keyValue)) {
                        keys.add(keyValue);
                        for (TreeMap<Object, Object> aggregatedData: data.values()) {
                            aggregatedData.put(keyValue, null);
                        }
                    }
                } else if (i==aggregateColumnIndex) {
                    aggregateValue = value;
                } else {
                    columnValues.add(rs.getObject(i));
                }
            }

            ObjectArrayKey objectKey = new ObjectArrayKey(columnValues.toArray());

            final TreeMap<Object, Object> rowData = data.getOrDefault( objectKey, emptyMapFromKeys(keys));
            if (aggregateClass.equals(BigDecimal.class)) {
                BigDecimal a = (BigDecimal) rowData.get(keyValue);
                BigDecimal b = (BigDecimal) aggregateValue;

                if (function == AggregateFunction.SUM) {
                    if (a == null) {
                        rowData.put(keyValue, b);
                    }
                    else if (b != null) {
                        rowData.put(keyValue, a.add(b));
                    }
                } else {
                    throw new UnsupportedOperationException("Only SUM is supported right now, sorry!");
                }
            } else {
                throw new UnsupportedOperationException("Only BigDecimals are supported right now, sorry!");
            }

            data.put(objectKey, rowData);
        }

        // build the final data cube
        for (Object k:keys) {
            columnNames.add( categoryFormat!= null ? categoryFormat.format(k) : k.toString());
        }

        Object[][] resultData = new Object[data.size()][columnNames.size()];
        int r = 0;
        for (Map.Entry<ObjectArrayKey, TreeMap<Object, Object>> e : data.entrySet()) {
            int c=0;
            for (Object k: e.getKey().keys) {
                resultData[r][c] = k;
                c++;
            }
            for (Object k: keys) {
                resultData[r][c] = e.getValue().get(k);
                c++;
            }
            r++;
        }

        return new Object[][] {
            columnNames.toArray(new String[columnNames.size()])
            , resultData
        };
    }
}
