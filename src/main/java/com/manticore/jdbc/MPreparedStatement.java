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
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MPreparedStatement implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(MPreparedStatement.class.getName());
    private static final int DEFAULT_BATCH_SIZE = 24;

    private final PreparedStatement statement;
    private final ParameterMetaData parameterMetaData;
    private final String sqlStr;
    private final CaseInsensitiveMap<String, MNamedParameter> parameters =
            new CaseInsensitiveMap<>();

    private long recordCount = 0;
    private final int batchSize;

    public MPreparedStatement(Connection conn, String sqlStr, int batchSize)
            throws SQLException, JSQLParserException {
        this.sqlStr = rewriteSqlStr(sqlStr);
        this.batchSize = batchSize;
        statement = conn.prepareStatement(this.sqlStr);
        parameterMetaData = statement.getParameterMetaData();

        setParameterTypes();
    }

    public MPreparedStatement(Connection conn, String sqlStr)
            throws SQLException, JSQLParserException {
        this(conn, sqlStr, DEFAULT_BATCH_SIZE);
    }

    /** The rewritten statement, with every named parameter replaced by a positional {@code ?}. */
    public String getSqlStr() {
        return sqlStr;
    }

    private String rewriteSqlStr(String sqlStr) throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement parsedStatement = CCJSqlParserUtil.parse(sqlStr);
        StringBuilder builder = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            int i = 1;

            @Override
            public <S> StringBuilder visit(JdbcParameter parameter, S context) {
                // positional parameters get a synthetic name matching their 1-based position
                return registerParameter(":" + i);
            }

            @Override
            public <S> StringBuilder visit(JdbcNamedParameter parameter, S context) {
                return registerParameter(parameter.getName());
            }

            private StringBuilder registerParameter(String id) {
                MNamedParameter existing = parameters.get(id);
                if (existing == null) {
                    parameters.put(id, new MNamedParameter(id, i));
                } else {
                    existing.add(i);
                }

                builder.append("?");
                i++;
                return builder;
            }
        };

        SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, builder);
        expressionDeParser.setSelectVisitor(selectDeParser);
        expressionDeParser.setBuilder(builder);

        StatementDeParser statementDeParser =
                new StatementDeParser(expressionDeParser, selectDeParser, builder);
        parsedStatement.accept(statementDeParser);

        return builder.toString();
    }

    private LinkedList<Object> getParamArr(Map<String, Object> parameterValues) {
        LinkedList<Object> objects = new LinkedList<>();
        for (MNamedParameter p : parameters.values()) {
            for (Integer position : p.getPositions()) {
                while (objects.size() < position) {
                    objects.add(null);
                }
                objects.set(position - 1, parameterValues.get(p.getId()));
            }
        }
        return objects;
    }

    private void setParameters(Map<String, Object> parameterValues) throws SQLException {
        setParameters(getParamArr(parameterValues).toArray());
    }

    private void setParameters(Object... parameterValues) throws SQLException {
        statement.clearParameters();
        int parameterIndex = 0;
        for (Object o : parameterValues) {
            parameterIndex++;
            setParameter(parameterIndex, o);
        }
    }

    private void setParameter(int parameterIndex, Object o) throws SQLException {
        try {
            switch (parameterMetaData.getParameterType(parameterIndex)) {
                case Types.TIMESTAMP:
                    setTimestampParameter(parameterIndex, o);
                    break;
                case Types.DATE:
                    setDateParameter(parameterIndex, o);
                    break;
                case Types.BINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    setBinaryParameter(parameterIndex, o);
                    break;

                // @todo: add more SQLType Mappings
                default:
                    statement.setObject(parameterIndex, o);
            }
        } catch (SQLException | RuntimeException ex) {
            // the driver did not accept the typed setter; let it coerce the value itself
            LOGGER.log(Level.FINE, ex,
                    () -> "Falling back to setObject() for parameter " + parameterIndex);
            statement.setObject(parameterIndex, o);
        }
    }

    private void setTimestampParameter(int parameterIndex, Object o) throws SQLException {
        if (o instanceof java.util.Date) {
            statement.setTimestamp(parameterIndex, MJdbcTools.getSQLTimestamp((java.util.Date) o));
        } else if (o instanceof Calendar) {
            statement.setTimestamp(parameterIndex, MJdbcTools.getSQLTimestamp((Calendar) o));
        } else {
            statement.setObject(parameterIndex, o);
        }
    }

    private void setDateParameter(int parameterIndex, Object o) throws SQLException {
        if (o instanceof java.util.Date) {
            statement.setDate(parameterIndex, MJdbcTools.getSQLDate((java.util.Date) o));
        } else if (o instanceof Calendar) {
            statement.setDate(parameterIndex, MJdbcTools.getSQLDate((Calendar) o));
        } else {
            statement.setObject(parameterIndex, o);
        }
    }

    private void setBinaryParameter(int parameterIndex, Object o) throws SQLException {
        if (o instanceof byte[]) {
            byte[] bytes = (byte[]) o;
            statement.setBinaryStream(parameterIndex, new ByteArrayInputStream(bytes),
                    bytes.length);
        } else {
            statement.setObject(parameterIndex, o);
        }
    }

    public boolean execute(Map<String, Object> parameterValues) throws SQLException {
        setParameters(parameterValues);
        return statement.execute();
    }

    public boolean execute(Object... parameterValues) throws SQLException {
        setParameters(parameterValues);
        return statement.execute();
    }

    public int executeUpdate(Map<String, Object> parameterValues) throws SQLException {
        setParameters(parameterValues);
        return statement.executeUpdate();
    }

    public int executeUpdate(Object... parameterValues) throws SQLException {
        setParameters(parameterValues);
        return statement.executeUpdate();
    }

    public ResultSet executeQuery(Map<String, Object> parameterValues) throws SQLException {
        setParameters(parameterValues);
        return statement.executeQuery();
    }

    public ResultSet executeQuery(Object... parameterValues) throws SQLException {
        setParameters(parameterValues);
        return statement.executeQuery();
    }

    @Override
    public void close() {
        try {
            statement.close();
        } catch (SQLException ex) {
            // nothing we can do here, but swallowing it silently hides driver problems
            LOGGER.log(Level.FINE, ex, () -> "Failed to close the PreparedStatement.");
        }
    }

    public void cancel() throws SQLException {
        statement.cancel();
    }

    public void addBatch(Map<String, Object> parameterValues) throws SQLException {
        setParameters(parameterValues);
        statement.addBatch();

        recordCount++;
    }

    public void addBatch(Object... parameterValues) throws SQLException {
        setParameters(parameterValues);
        statement.addBatch();

        recordCount++;
    }

    public int[] executeBatch() throws SQLException {
        recordCount = 0;
        return statement.executeBatch();
    }

    /** Adds a record and flushes the batch whenever {@code batchSize} records have accumulated. */
    public int[] addAndExecuteBatch(Map<String, Object> parameterValues) throws SQLException {
        addBatch(parameterValues);

        return recordCount % batchSize == 0 ? executeBatch() : new int[0];
    }

    /** Adds a record and flushes the batch whenever {@code batchSize} records have accumulated. */
    public int[] addAndExecuteBatch(Object... parameterValues) throws SQLException {
        addBatch(parameterValues);

        return recordCount % batchSize == 0 ? executeBatch() : new int[0];
    }

    public void clearBatch() throws SQLException {
        recordCount = 0;
        statement.clearBatch();
    }

    public ResultSet getResultSet() throws SQLException {
        return statement.getResultSet();
    }

    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    private void setParameterTypes() throws SQLException {
        for (int i = 1; i <= parameterMetaData.getParameterCount(); i++) {
            for (MNamedParameter p : parameters.values()) {
                if (p.getFirstPosition() == i) {
                    p.setType(parameterMetaData.getParameterType(i),
                            parameterMetaData.getParameterTypeName(i),
                            parameterMetaData.getParameterClassName(i),
                            parameterMetaData.getPrecision(i),
                            parameterMetaData.getScale(i),
                            parameterMetaData.isNullable(i));
                }
            }
        }
    }

    public List<MNamedParameter> getNamedParametersByAppearance() {
        LinkedList<MNamedParameter> values = new LinkedList<>(parameters.values());
        values.sort(Comparator.comparingInt(MNamedParameter::getFirstPosition));
        return values;
    }

    public List<MNamedParameter> getNamedParametersByName() {
        LinkedList<MNamedParameter> values = new LinkedList<>(parameters.values());
        values.sort(Comparator.comparing(MNamedParameter::getId, String::compareToIgnoreCase));
        return values;
    }
}
