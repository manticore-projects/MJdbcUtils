/**
 * Copyright (C) 2026 manticore-projects Co. Ltd. <support@manticore-projects.com>
 * SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Format;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class MJdbcTools {
    public static final DateTimeFormatter SQL_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private MJdbcTools() {
        // utility class, not meant to be instantiated
    }

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
        return "{d '" + SQL_DATE_FORMAT.format(toLocalDate(d.getTime())) + "'}";
    }

    public static String getSQLDateStr(Calendar c) {
        return getSQLDateStr(c.getTime());
    }

    public static String getSQLDateTimeStr(Date d) {
        return "{ts '" + SQL_TIMESTAMP_FORMAT.format(toLocalDateTime(d.getTime())) + "'}";
    }

    public static String getSQLDateTimeStr(java.sql.Timestamp ts) {
        return "{ts '" + SQL_TIMESTAMP_FORMAT.format(toLocalDateTime(ts.getTime())) + "'}";
    }

    public static String getSQLDateTimeStr(Calendar c) {
        return getSQLDateTimeStr(c.getTime());
    }

    // java.sql.Date and java.sql.Time both throw on toInstant(), so always go through the epoch
    private static LocalDate toLocalDate(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli).atZone(ZoneId.systemDefault()).toLocalDate();
    }

    private static LocalDateTime toLocalDateTime(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * Returns the SHA-256 digest of the normalised (parsed and re-written) statement as a lower
     * case hex string, suitable for recognising semantically identical statements.
     */
    public static String getSQLHash(String sqlStr)
            throws JSQLParserException, NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);
        messageDigest.update(statement.toString().getBytes(StandardCharsets.UTF_8));

        return toHexString(messageDigest.digest());
    }

    private static String toHexString(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            builder.append(HEX[(b >> 4) & 0xF]).append(HEX[b & 0xF]);
        }
        return builder.toString();
    }

    /** Renders a parameter value as a SQL literal, escaping single quotes and ampersands. */
    private static String getParameterStr(Object o) {
        if (o == null) {
            return "NULL";
        } else if (o instanceof java.sql.Date) {
            return "{d '" + o + "'}";
        } else if (o instanceof Date) {
            Calendar cal = GregorianCalendar.getInstance();
            cal.setTime((Date) o);
            return getTemporalStr(cal);
        } else if (o instanceof Calendar) {
            return getTemporalStr((Calendar) o);
        } else if (o instanceof Number) {
            return getNumberStr((Number) o);
        } else if (o instanceof Boolean) {
            return (Boolean) o ? "TRUE" : "FALSE";
        } else {
            return getTextStr(o.toString());
        }
    }

    /** A date literal when the time of day is exactly midnight, a timestamp literal otherwise. */
    private static String getTemporalStr(Calendar cal) {
        boolean midnight = cal.get(Calendar.MILLISECOND) == 0
                && cal.get(Calendar.SECOND) == 0
                && cal.get(Calendar.MINUTE) == 0
                && cal.get(Calendar.HOUR_OF_DAY) == 0;

        return midnight
                ? "{d '" + SQL_DATE_FORMAT.format(toLocalDate(cal.getTimeInMillis())) + "'}"
                : "{ts '" + SQL_TIMESTAMP_FORMAT.format(toLocalDateTime(cal.getTimeInMillis()))
                        + "'}";
    }

    private static String getNumberStr(Number n) {
        // avoid the scientific notation of BigDecimal.toString()
        return n instanceof BigDecimal ? ((BigDecimal) n).toPlainString() : n.toString();
    }

    private static String getTextStr(String s) {
        // '&' would otherwise be swallowed as a substitution variable by SQL*Plus and friends
        return "'" + s.replace("'", "''").replace("&", "' || chr(38) || '") + "'";
    }

    /** Result of a streaming parameter scan, see {@link #scanParameters(Reader)}. */
    public static final class ParameterScan {
        private final long statementCount;
        private final Set<String> namedParameters;
        private final long positionalParameterCount;

        ParameterScan(long statementCount, Set<String> namedParameters,
                long positionalParameterCount) {
            this.statementCount = statementCount;
            this.namedParameters = Collections.unmodifiableSet(namedParameters);
            this.positionalParameterCount = positionalParameterCount;
        }

        public long getStatementCount() {
            return statementCount;
        }

        /** Distinct named parameters in first-seen order. */
        public Set<String> getNamedParameters() {
            return namedParameters;
        }

        public long getPositionalParameterCount() {
            return positionalParameterCount;
        }
    }

    /**
     * Returns the distinct named parameters of a single parseable statement, in first-seen order,
     * based on a full JSqlParser parse (so {@code :names} inside literals or comments are never
     * reported).
     */
    public static Set<String> getNamedParameters(String sqlStr) throws JSQLParserException {
        final LinkedHashSet<String> names = new LinkedHashSet<>();
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);
        StringBuilder builder = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            @Override
            public <S> StringBuilder visit(JdbcNamedParameter parameter, S context) {
                names.add(parameter.getName());
                return super.visit(parameter, context);
            }
        };

        SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, builder);
        expressionDeParser.setSelectVisitor(selectDeParser);
        expressionDeParser.setBuilder(builder);

        StatementDeParser statementDeParser =
                new StatementDeParser(expressionDeParser, selectDeParser, builder);
        statement.accept(statementDeParser);

        return names;
    }

    /** See {@link #scanParameters(Reader, boolean)}, with PL/SQL slash termination enabled. */
    public static ParameterScan scanParameters(Reader reader) throws IOException {
        return scanParameters(reader, true);
    }

    /**
     * Streams through an arbitrarily large SQL script in O(1) memory and returns the number of
     * statements plus all distinct parameters. Intended as the first pass of a two-pass batch
     * execution: scan (count and parameters), prompt, then re-open the source and execute.
     * <p>
     * Statements are split by {@link MSqlSplitter}. Parameter detection is two-tiered: the splitter
     * collects lexical candidates ({@code :name} / {@code ?} outside literals and comments);
     * statements without candidates are never parsed (dump files scan at I/O speed). Statements
     * with candidates are confirmed by a full JSqlParser parse; where the grammar does not cover
     * the statement (PL/SQL blocks, vendor DDL), the lexical candidates are used as fallback --
     * except {@code :NEW}/{@code :OLD} correlation names inside PL/SQL blocks, which are trigger
     * pseudo-records, not bind parameters.
     *
     * @param reader the script source; not closed by this method
     * @param plsqlSlashTermination see {@link MSqlSplitter}
     */
    public static ParameterScan scanParameters(Reader reader, boolean plsqlSlashTermination)
            throws IOException {
        MSqlSplitter splitter = new MSqlSplitter(reader, plsqlSlashTermination);
        long statementCount = 0;
        long positionalCount = 0;
        LinkedHashSet<String> names = new LinkedHashSet<>();

        String sqlStr;
        while ((sqlStr = splitter.nextStatement()) != null) {
            statementCount++;
            positionalCount += splitter.getPositionalParameterCount();

            Set<String> candidates = splitter.getNamedParameters();
            if (candidates.isEmpty()) {
                continue;
            }
            try {
                names.addAll(getNamedParameters(sqlStr));
            } catch (JSQLParserException | RuntimeException ex) {
                // grammar does not cover this statement: fall back to the lexical candidates
                addLexicalCandidates(names, candidates, splitter.isPlSqlStatement());
            }
        }
        return new ParameterScan(statementCount, names, positionalCount);
    }

    private static void addLexicalCandidates(Set<String> names, Set<String> candidates,
            boolean plsqlStatement) {
        for (String candidate : candidates) {
            boolean correlationName = plsqlStatement
                    && ("NEW".equalsIgnoreCase(candidate) || "OLD".equalsIgnoreCase(candidate));
            if (!correlationName) {
                names.add(candidate);
            }
        }
    }

    public static String rewriteStatementWithNamedParameters(String sqlStr,
            Map<String, Object> parameters) throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);
        StringBuilder builder = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            @Override
            public <S> StringBuilder visit(JdbcNamedParameter parameter, S context) {
                builder.append(getParameterStr(parameters.get(parameter.getName())));
                return builder;
            }
        };

        SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, builder);
        expressionDeParser.setSelectVisitor(selectDeParser);
        expressionDeParser.setBuilder(builder);

        StatementDeParser statementDeParser =
                new StatementDeParser(expressionDeParser, selectDeParser, builder);
        statement.accept(statementDeParser);

        return builder.toString();
    }

    public static String rewriteStatementWithNamedParameters(String sqlStr, Object... parameters)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);
        StringBuilder builder = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            int i = 0;

            @Override
            public <S> StringBuilder visit(JdbcParameter parameter, S context) {
                builder.append(getParameterStr(parameters[i]));
                i++;
                return builder;
            }

            @Override
            public <S> StringBuilder visit(JdbcNamedParameter parameter, S context) {
                builder.append(getParameterStr(parameters[i]));
                i++;
                return builder;
            }
        };

        SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, builder);
        expressionDeParser.setSelectVisitor(selectDeParser);
        expressionDeParser.setBuilder(builder);

        StatementDeParser statementDeParser =
                new StatementDeParser(expressionDeParser, selectDeParser, builder);
        statement.accept(statementDeParser);

        return builder.toString();
    }

    public enum AggregateFunction {
        SUM, COUNT
    }

    /**
     * The getPivotFromQuery function takes a ResultSet and converts it into a pivot table. The
     * function is designed to create columns for each key of the Category Column and to aggregate
     * the values of the Aggregate Column for each Category.
     *
     * @param rs The ResultSet holding the source data with the category values in rows
     * @param function Determine what type of aggregate function to use (SUM, COUNT, ...)
     * @param aggregateColumnName Specify the column name of the aggregate value
     * @param categoryColumnName Identify the column that will be transformed into separate Value
     *        Columns
     * @param categoryFormat Format the key values into column labels
     * @param buildTotals If to insert Total rows below and column on the right side
     * @param repeatHeader If to insert the header repeatedly before each category. <b>Not
     *        implemented</b>, this argument is currently ignored.
     * @return A 2-dimensional array holding the transformed Column Names and the Data
     */
    public static Object[][] getPivotFromQuery(ResultSet rs, AggregateFunction function,
            String aggregateColumnName, String categoryColumnName, Format categoryFormat,
            boolean buildTotals, boolean repeatHeader) throws SQLException {
        return new MPivotTable(function, aggregateColumnName, categoryColumnName, categoryFormat,
                buildTotals).build(rs);
    }
}
