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

import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;

public class MNamedParameter implements Comparable<MNamedParameter> {
    public String id;
    public String label;

    public Object value;
    public TreeSet<Integer> positions = new TreeSet<>();

    public static LinkedList<Object> getParameterValueArray(Collection<MNamedParameter> parameters) {
        LinkedList<Object> objects = new LinkedList<>();
        for (MNamedParameter p : parameters)
            for (Integer position : p.positions) {
                while (objects.size() < position) {
                    objects.add(null);
                }
                objects.set(position - 1, p.value);
            }
        return objects;
    }

    public MNamedParameter(String id, int position) {
        this.id = id.toUpperCase();
        positions.add(position);
    }

    public void add(Integer position) {
        positions.add(position);
    }

    @Override
    public int compareTo(MNamedParameter o) {
        return id.compareToIgnoreCase(o.id);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final MNamedParameter other = (MNamedParameter) obj;
        return !((this.id == null) ? (other.id != null) : !this.id.equalsIgnoreCase(other.id));
    }

    @Override
    public String toString() {
        return this.label != null && this.label.length() > 0 ? this.label : this.id;
    }
}
