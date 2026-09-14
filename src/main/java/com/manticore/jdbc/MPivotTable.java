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

import com.manticore.jdbc.MJdbcTools.AggregateFunction;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Turns a flat {@link ResultSet} into a pivot table: the distinct values of the category column
 * become columns of their own and the aggregate column is accumulated per remaining row key.
 * <p>
 * One instance builds exactly one table and is not reusable or thread safe; see
 * {@link MJdbcTools#getPivotFromQuery(ResultSet, AggregateFunction, String, String, Format, boolean, boolean)}
 * for the public entry point.
 */
final class MPivotTable {

    private final AggregateFunction function;
    private final String aggregateColumnName;
    private final String categoryColumnName;
    private final Format categoryFormat;
    private final boolean buildTotals;

    private final List<String> columnNames = new ArrayList<>();
    private final TreeSet<Object> keys = new TreeSet<>();
    private final TreeMap<ObjectArrayKey, TreeMap<Object, Object>> data = new TreeMap<>();

    private int columnCount;
    private int categoryColumnIndex = -1;
    private int aggregateColumnIndex = -1;
    private Class<?> categoryClass;
    private Class<?> aggregateClass;

    MPivotTable(AggregateFunction function, String aggregateColumnName, String categoryColumnName,
            Format categoryFormat, boolean buildTotals) {
        this.function = function;
        this.aggregateColumnName = aggregateColumnName;
        this.categoryColumnName = categoryColumnName;
        this.categoryFormat = categoryFormat;
        this.buildTotals = buildTotals;
    }

    /** @return {@code { String[] columnNames, Object[][] data }} */
    Object[][] build(ResultSet rs) throws SQLException {
        readMetaData(rs.getMetaData());
        readRows(rs);
        appendCategoryColumnNames();

        return new Object[][] {
                columnNames.toArray(new String[0]),
                buildRows()
        };
    }

    private void readMetaData(ResultSetMetaData metaData) throws SQLException {
        columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            if (columnName.equalsIgnoreCase(categoryColumnName)) {
                categoryColumnIndex = i;
                categoryClass = getColumnClass(metaData, i);
            } else if (columnName.equalsIgnoreCase(aggregateColumnName)) {
                aggregateColumnIndex = i;
                aggregateClass = getColumnClass(metaData, i);
            } else {
                columnNames.add(columnName);
            }
        }

        if (categoryClass == null || aggregateClass == null) {
            throw new IllegalArgumentException("Unable to determine the classes of the Category"
                    + " column '" + categoryColumnName + "' and the Aggregate column '"
                    + aggregateColumnName + "' within the given ResultSet.");
        }
    }

    private static Class<?> getColumnClass(ResultSetMetaData metaData, int i) throws SQLException {
        String className = metaData.getColumnClassName(i);
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException ignored) {
            // the driver reports a class we cannot see; readMetaData() will report it
            return null;
        }
    }

    private void readRows(ResultSet rs) throws SQLException {
        List<Object> columnValues = new ArrayList<>();
        while (rs.next()) {
            columnValues.clear();
            Object keyValue = null;
            Object aggregateValue = null;

            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                if (i == categoryColumnIndex) {
                    keyValue = value;
                    registerKey(keyValue);
                } else if (i == aggregateColumnIndex) {
                    aggregateValue = value;
                } else {
                    columnValues.add(value);
                }
            }

            accumulate(new ObjectArrayKey(columnValues.toArray()), keyValue, aggregateValue);
        }
    }

    /** Registers a category value and extends the already aggregated rows with it. */
    private void registerKey(Object keyValue) {
        if (keys.add(keyValue)) {
            for (TreeMap<Object, Object> aggregatedData : data.values()) {
                aggregatedData.put(keyValue, null);
            }
        }
    }

    private void accumulate(ObjectArrayKey objectKey, Object keyValue, Object aggregateValue) {
        if (!BigDecimal.class.equals(aggregateClass)) {
            throw new UnsupportedOperationException(
                    "Only BigDecimals are supported right now, sorry!");
        }
        if (function != AggregateFunction.SUM) {
            throw new UnsupportedOperationException("Only SUM is supported right now, sorry!");
        }

        TreeMap<Object, Object> rowData = data.computeIfAbsent(objectKey, k -> emptyRow());
        BigDecimal aggregated = (BigDecimal) rowData.get(keyValue);
        BigDecimal value = (BigDecimal) aggregateValue;

        if (aggregated == null) {
            rowData.put(keyValue, value);
        } else if (value != null) {
            rowData.put(keyValue, aggregated.add(value));
        }
    }

    private TreeMap<Object, Object> emptyRow() {
        TreeMap<Object, Object> map = new TreeMap<>();
        for (Object k : keys) {
            map.put(k, null);
        }
        return map;
    }

    private void appendCategoryColumnNames() {
        for (Object k : keys) {
            columnNames.add(categoryFormat != null ? categoryFormat.format(k) : String.valueOf(k));
        }
        if (buildTotals) {
            columnNames.add("Total");
        }
    }

    private Object[][] buildRows() {
        List<Object[]> resultData = new ArrayList<>();
        BigDecimal[] subTotals = new BigDecimal[keys.size() + 1];
        Arrays.fill(subTotals, BigDecimal.ZERO);

        Object mainCategory = null;
        int row = 0;
        for (Map.Entry<ObjectArrayKey, TreeMap<Object, Object>> e : data.entrySet()) {
            Object[] rowData = new Object[columnNames.size()];
            int offset = e.getKey().copyInto(rowData);

            if (buildTotals && !Objects.equals(rowData[0], mainCategory)) {
                if (mainCategory != null) {
                    resultData.add(buildTotalRow(mainCategory, e.getKey(), subTotals));
                    resultData.add(new Object[columnNames.size()]);
                }
                mainCategory = rowData[0];
                Arrays.fill(subTotals, BigDecimal.ZERO);
            }

            fillAggregates(rowData, offset, e.getValue(), subTotals);
            resultData.add(rowData);

            // totals after the last row
            if (buildTotals && ++row == data.size()) {
                resultData.add(buildTotalRow(mainCategory, e.getKey(), subTotals));
            }
        }

        return resultData.toArray(new Object[0][]);
    }

    private void fillAggregates(Object[] rowData, int offset, Map<Object, Object> aggregatedData,
            BigDecimal[] subTotals) {
        BigDecimal rowTotal = BigDecimal.ZERO;
        int c = offset;
        int t = 0;
        for (Object k : keys) {
            BigDecimal value = (BigDecimal) aggregatedData.get(k);
            rowData[c] = value;

            if (buildTotals && value != null) {
                rowTotal = rowTotal.add(value);
                subTotals[t] = subTotals[t].add(value);
            }
            c++;
            t++;
        }
        if (buildTotals) {
            rowData[c] = rowTotal;
            subTotals[t] = subTotals[t].add(rowTotal);
        }
    }

    private Object[] buildTotalRow(Object mainCategory, ObjectArrayKey key,
            BigDecimal[] subTotals) {
        Object[] totalRowData = new Object[columnNames.size()];
        totalRowData[0] = mainCategory;
        totalRowData[key.size() - 2] = "Total";
        System.arraycopy(subTotals, 0, totalRowData, key.size(), keys.size() + 1);
        return totalRowData;
    }

    /** The remaining (non category, non aggregate) column values of a row, used as the row key. */
    private static final class ObjectArrayKey implements Comparable<ObjectArrayKey> {
        private final Object[] keys;

        ObjectArrayKey(Object[] keys) {
            this.keys = keys.clone();
        }

        int size() {
            return keys.length;
        }

        int copyInto(Object[] target) {
            System.arraycopy(keys, 0, target, 0, keys.length);
            return keys.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return Arrays.deepEquals(keys, ((ObjectArrayKey) o).keys);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(keys);
        }

        @Override
        public int compareTo(ObjectArrayKey other) {
            int commonLength = Math.min(keys.length, other.keys.length);
            for (int i = 0; i < commonLength; i++) {
                int elementComparison = compare(keys[i], other.keys[i]);
                if (elementComparison != 0) {
                    return elementComparison;
                }
            }
            // shorter keys sort first; equal length and equal elements means equal keys
            return Integer.compare(keys.length, other.keys.length);
        }

        @SuppressWarnings("unchecked")
        private static int compare(Object a, Object b) {
            if (a == b) {
                return 0;
            } else if (a == null) {
                return -1;
            } else if (b == null) {
                return 1;
            }
            return ((Comparable<Object>) a).compareTo(b);
        }
    }
}
