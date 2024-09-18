/**
 * Copyright (C) 2024 manticore-projects Co. Ltd. <support@manticore-projects.com>
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

public final class MPreparedStatement implements Closeable {
    private final static int DEFAULT_BATCH_SIZE = 24;
    private final PreparedStatement statement;
    private final ParameterMetaData parameterMetaData;
    private final String sqlStr;
    private final CaseInsensitiveMap<String, MNamedParameter> parameters =
            new CaseInsensitiveMap<>();

    private long recordCount = 0;
    private final int batchSize;

    private String rewriteSqlStr(String sqlStr) throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sqlStr);
        StringBuilder builder = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            int i = 1;

            @Override
            public <S> StringBuilder visit(JdbcParameter parameter, S context) {
                String id = ":" + (i + 1);
                if (!parameters.containsKey(id)) {
                    parameters.put(id, new MNamedParameter(id, i));
                } else {
                    parameters.get(id).add(i);
                }

                buffer.append("?");
                i++;
                return buffer;
            }

            @Override
            public <S> StringBuilder visit(JdbcNamedParameter parameter, S context) {
                String id = parameter.getName();
                if (!parameters.containsKey(id)) {
                    parameters.put(id, new MNamedParameter(id, i));
                } else {
                    parameters.get(id).add(i);
                }

                buffer.append("?");
                i++;

                return buffer;
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

    private void setParameters(Map<String, Object> parameterValues) throws SQLException {
        setParameters(getParamArr(parameterValues).toArray());
    }

    private void setParameters(Object... parameterValues) throws SQLException {
        statement.clearParameters();
        int parameterIndex = 0;
        for (Object o : parameterValues) {
            parameterIndex++;
            try {
                int parameterType = parameterMetaData.getParameterType(parameterIndex);
                switch (parameterType) {
                    case Types.TIMESTAMP:
                        if (o instanceof java.util.Date) {
                            java.util.Date date = (java.util.Date) o;
                            statement.setTimestamp(parameterIndex,
                                    MJdbcTools.getSQLTimestamp(date));
                        } else if (o instanceof Calendar) {
                            Calendar calendar = (Calendar) o;
                            statement.setTimestamp(parameterIndex,
                                    MJdbcTools.getSQLTimestamp(calendar));
                        } else {
                            statement.setObject(parameterIndex, o);
                        }
                        break;
                    case Types.DATE:
                        if (o instanceof java.util.Date) {
                            java.util.Date date = (java.util.Date) o;
                            statement.setDate(parameterIndex, MJdbcTools.getSQLDate(date));
                        } else if (o instanceof Calendar) {
                            Calendar calendar = (Calendar) o;
                            statement.setDate(parameterIndex, MJdbcTools.getSQLDate(calendar));
                        } else {
                            statement.setObject(parameterIndex, o);
                        }
                        break;
                    case Types.BINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        if (o instanceof byte[]) {
                            byte[] bytes = (byte[]) o;
                            statement.setBinaryStream(parameterIndex,
                                    new ByteArrayInputStream(bytes), bytes.length);
                        } else {
                            statement.setObject(parameterIndex, o);
                        }
                        break;

                    // @todo: add more SQLType Mappings
                    default:
                        statement.setObject(parameterIndex, o);
                }
            } catch (Exception ignore) {
                statement.setObject(parameterIndex, o);
            }
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

    public void close() {
        try {
            statement.close();
        } catch (Exception ignore) {
            // nothing we can do here
        }
    }

    public void cancel() throws SQLException {
        statement.cancel();
    }


    public void addBatch(Map<String, Object> parameterValues) throws SQLException {
        statement.clearParameters();
        setParameters(parameterValues);
        statement.addBatch();

        recordCount++;
    }

    public void addBatch(Object... parameterValues) throws SQLException {
        statement.clearParameters();
        setParameters(parameterValues);
        statement.addBatch();
    }

    public int[] executeBatch() throws SQLException {
        recordCount = 0;
        return statement.executeBatch();
    }

    public int[] addAndExecuteBatch(Map<String, Object> parameterValues) throws SQLException {
        addBatch(parameterValues);

        if (recordCount % batchSize == 0) {
            return executeBatch();
        } else {
            return new int[0];
        }
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
                if (p.getPositions().first() == i) {
                    int type = parameterMetaData.getParameterType(i);
                    String typeName = parameterMetaData.getParameterTypeName(i);
                    int precision = parameterMetaData.getPrecision(i);
                    int scale = parameterMetaData.getScale(i);
                    int nullable = parameterMetaData.isNullable(i);
                    String className = parameterMetaData.getParameterClassName(i);

                    p.setType(type, typeName, className, precision, scale, nullable);
                }
            }
        }
    }

    public List<MNamedParameter> getNamedParametersByAppearance() {
        LinkedList<MNamedParameter> values = new LinkedList<>(parameters.values());
        Comparator<MNamedParameter> comparator = new Comparator<MNamedParameter>() {
            @Override
            public int compare(MNamedParameter o1, MNamedParameter o2) {
                return Integer.compare(o1.getPositions().first(), o2.getPositions().first());
            }
        };
        values.sort(comparator);
        return values;
    }

    public List<MNamedParameter> getNamedParametersByName() {
        LinkedList<MNamedParameter> values = new LinkedList<>(parameters.values());
        Comparator<MNamedParameter> comparator = new Comparator<MNamedParameter>() {
            @Override
            public int compare(MNamedParameter o1, MNamedParameter o2) {
                return o1.getId().compareToIgnoreCase(o2.getId());
            }
        };
        values.sort(comparator);
        return values;
    }
}
