package com.manticore.jdbc;

import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MNamedParameter implements Comparable<MNamedParameter> {
    public String id;
    public String label;

    public Object value;
    public TreeSet<Integer> positions = new TreeSet<>();

    public final static Pattern NAMED_PARAMETER_PATTERN = Pattern.compile("[^a-zA-Z0-9]\\:(\\w*)\\b");

    public static Collection<MNamedParameter> getParameterSet(String sqlStr) {
        CaseInsensitiveMap<String, MNamedParameter> parameterMap = new CaseInsensitiveMap<>();
        if (sqlStr != null && sqlStr.trim().length() > 0) {
            Matcher matcher = NAMED_PARAMETER_PATTERN.matcher(sqlStr);
            int i = 0;
            while (matcher.find()) {
                i++;
                String pName = matcher.group(1).toLowerCase();
                if (!parameterMap.containsKey(pName)) {
                    parameterMap.put(pName, new MNamedParameter(pName, i));
                } else parameterMap.get(pName).add(i);
            }
        }
        return parameterMap.values();
    }

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
