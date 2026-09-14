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

import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;
import java.util.TreeSet;

public class MNamedParameter implements Comparable<MNamedParameter> {
    private final String id;
    private String label;

    private Object value;
    private final TreeSet<Integer> positions = new TreeSet<>();

    private Integer type = null;
    private String typeName = null;
    private Integer precision = null;
    private Integer scale = null;
    private Integer nullable = null;
    private String className = null;

    public static LinkedList<Object> getParameterValueArray(
            Collection<MNamedParameter> parameters) {
        LinkedList<Object> objects = new LinkedList<>();
        for (MNamedParameter p : parameters) {
            for (Integer position : p.positions) {
                while (objects.size() < position) {
                    objects.add(null);
                }
                objects.set(position - 1, p.value);
            }
        }
        return objects;
    }

    public MNamedParameter(String id, int position) {
        // normalise once, so that equals(), hashCode() and compareTo() stay consistent
        this.id = id.toUpperCase(Locale.ROOT);
        positions.add(position);
    }

    public void add(Integer position) {
        positions.add(position);
    }

    @Override
    public int compareTo(MNamedParameter o) {
        return id.compareTo(o.id);
    }

    @Override
    public int hashCode() {
        return 67 * 3 + id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return id.equals(((MNamedParameter) obj).id);
    }

    @Override
    public String toString() {
        return this.label != null && !this.label.isEmpty() ? this.label : this.id;
    }


    public void setType(int type, String typeName, String className, int precision, int scale,
            int nullable) {
        this.type = type;
        this.typeName = typeName;
        this.className = className;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    public Integer getScale() {
        return scale;
    }

    public String getId() {
        return id;
    }

    public MNamedParameter setLabel(String label) {
        this.label = label;
        return this;
    }

    public MNamedParameter setValue(Object value) {
        this.value = value;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public Object getValue() {
        return value;
    }

    public TreeSet<Integer> getPositions() {
        return new TreeSet<>(positions);
    }

    /** The first (1-based) position of this parameter, without copying the whole position set. */
    public int getFirstPosition() {
        return positions.first();
    }

    public Integer getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getNullable() {
        return nullable;
    }

    public String getClassName() {
        return className;
    }
}
