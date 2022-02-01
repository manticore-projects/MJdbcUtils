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

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MPreparedStatementTest {

    private final Connection conn;

    public MPreparedStatementTest() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement statement = conn.createStatement()) {
            statement.execute("CREATE TABLE test ( a DECIMAL(3) PRIMARY KEY, b VARCHAR(128) NOT NULL, c DATE NOT NULL, d TIMESTAMP NOT NULL, e DECIMAL(23,5) NOT NULL ) ");
        }
    }

    private static CaseInsensitiveMap<String, Object> toMap(Object... values) throws Exception {
        if (values.length % 2 == 0) {
            CaseInsensitiveMap<String, Object> map = new CaseInsensitiveMap<>();
            for (int i = 0; i < values.length; i += 2) {
                map.put(values[i].toString(), values[i + 1]);
            }
            return map;
        } else {
            throw new Exception("Value List of odd size " + values.length + " is mot balanced.");
        }
    }

    @AfterAll
    private void closeConnection() {
        try {
            conn.close();
        } catch (SQLException ignore) {
        }
    }

    @Test
    public void executeTest() throws Exception {
        String ddlStr = "INSERT INTO test VALUES ( :a, :b, :c, :d, :e )";
        String qryStr = "SELECT Count(*) FROM test WHERE a = :a or b = :b";

        Map<String, Object> parameters = toMap("a", 1, "b", "Test String", "c", new Date(), "d", new Date(), "e", "0.12345");

        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() throws Throwable {
                MPreparedStatement st = new MPreparedStatement(conn, ddlStr);
                st.execute(parameters);
            }
        });

        try (
                MPreparedStatement st = new MPreparedStatement(conn, qryStr);
                ResultSet rs = st.executeQuery(parameters);
                ) {
            rs.next();
            Assertions.assertEquals(1, rs.getInt(1));
        }
    }

}
