package com.manticore.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TreeSet;

public class MPreparedStatement {
    private final PreparedStatement statement;
    private final String sqlStr;
    private final TreeSet<MNamedParameter> parammeters= new TreeSet<>();

    private final boolean tainted;

    public MPreparedStatement(Connection conn, String sqlStr) throws SQLException {
        this.sqlStr = sqlStr;
        this.tainted = true;
        statement = conn.prepareStatement(sqlStr);
    }
}
