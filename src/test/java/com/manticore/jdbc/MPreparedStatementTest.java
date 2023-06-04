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

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MPreparedStatementTest {
    private final static Logger LOGGER = Logger.getLogger(MPreparedStatementTest.class.getName());
    private static Connection conn;

    public MPreparedStatementTest() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement statement = conn.createStatement()) {
            statement.execute(
                    "CREATE TABLE test ( a DECIMAL(3) PRIMARY KEY, b VARCHAR(128) NOT NULL, c DATE NOT NULL, d TIMESTAMP NOT NULL, e DECIMAL(23,5) NOT NULL ) ");
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

    @BeforeEach
    public void truncateTable() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("TRUNCATE table test");
        }
    }

    @AfterAll
    public static void closeConnection() {
        try {
            conn.close();
        } catch (SQLException ignore) {
            // nothing we can do here
        }
    }

    @Test
    public void execute() throws Exception {
        String ddlStr = "INSERT INTO test VALUES ( :a, :b, :c, :d, :e )";
        String qryStr = "SELECT Count(*) FROM test WHERE a = :a or b = :b";

        Map<String, Object> parameters =
                toMap("a", 1, "b", "Test String", "c", new Date(), "d", new Date(), "e", "0.12345");

        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() throws Throwable {
                MPreparedStatement st = new MPreparedStatement(conn, ddlStr);
                Assertions.assertFalse(st.execute(parameters));
                Assertions.assertEquals(1, st.getUpdateCount());
            }
        });

        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() throws Throwable {
                try (
                        MPreparedStatement st = new MPreparedStatement(conn, qryStr);
                        ResultSet rs = st.executeQuery(parameters);) {
                    rs.next();
                    Assertions.assertEquals(1, rs.getInt(1));
                }
            }
        });
    }

    @Test
    public void addAndExecuteBatch() throws Exception {
        int maxRecords = 100;
        int batchSize = 4;
        String ddlStr = "INSERT INTO test VALUES ( :a, :b, :c, :d, :e )";
        String qryStr = "SELECT Count(*) FROM test";

        Map<String, Object> parameters =
                toMap("a", 1, "b", "Test String", "c", new Date(), "d", new Date(), "e", "0.12345");

        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() throws Throwable {
                MPreparedStatement st = new MPreparedStatement(conn, ddlStr, batchSize);

                for (int i = 0; i < maxRecords; i++) {
                    parameters.put("a", i);
                    parameters.put("b", "Test String " + i);

                    int[] results = st.addAndExecuteBatch(parameters);
                    int expectedArrLength = (i + 1) % batchSize == 0 ? batchSize : 0;

                    Assertions.assertEquals(expectedArrLength, results.length);
                }
                st.executeBatch();
            }
        });

        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() throws Throwable {
                try (
                        MPreparedStatement st = new MPreparedStatement(conn, qryStr);
                        ResultSet rs = st.executeQuery(parameters);) {
                    rs.next();
                    Assertions.assertEquals(maxRecords, rs.getInt(1));
                }
            }
        });
    }

    @Test
    public void getNamedParametersByAppearance() {
        String qryStr =
                "SELECT * FROM test WHERE d = :d and c = :c and b = :b and a = :a and e = :e";

        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() throws Throwable {
                try (
                        MPreparedStatement st = new MPreparedStatement(conn, qryStr);) {
                    List<MNamedParameter> parameters = st.getNamedParametersByAppearance();

                    Assertions.assertEquals("D", parameters.get(0).getId());
                    Assertions.assertEquals(java.sql.Timestamp.class.getName(),
                            parameters.get(0).getClassName());

                    Assertions.assertEquals("C", parameters.get(1).getId());
                    Assertions.assertEquals(java.sql.Date.class.getName(),
                            parameters.get(1).getClassName());

                    Assertions.assertEquals("B", parameters.get(2).getId());
                    Assertions.assertEquals(String.class.getName(),
                            parameters.get(2).getClassName());

                    Assertions.assertEquals("A", parameters.get(3).getId());
                    Assertions.assertEquals(java.math.BigDecimal.class.getName(),
                            parameters.get(3).getClassName());
                    Assertions.assertEquals(3, parameters.get(3).getPrecision());
                    Assertions.assertEquals(0, parameters.get(3).getScale());

                    Assertions.assertEquals("E", parameters.get(4).getId());
                    Assertions.assertEquals(23, parameters.get(4).getPrecision());
                    Assertions.assertEquals(5, parameters.get(4).getScale());

                    StringBuilder builder =
                            new StringBuilder("Found Named Parameters (ordered by appearance:\n");
                    for (MNamedParameter p : parameters) {
                        builder.append(p.getId()).append("\t").append(p.getClassName())
                                .append("\n");
                    }
                    LOGGER.info(builder.toString());
                }
            }
        });
    }

    @Test
    public void getNamedParametersByName() {
        String qryStr =
                "SELECT * FROM test WHERE d = :d and c = :c and b = :b and a = :a and e = :e";

        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() throws Throwable {
                try (
                        MPreparedStatement st = new MPreparedStatement(conn, qryStr);) {
                    List<MNamedParameter> parameters = st.getNamedParametersByName();
                    Assertions.assertEquals("A", parameters.get(0).getId());
                    Assertions.assertEquals("B", parameters.get(1).getId());
                    Assertions.assertEquals("C", parameters.get(2).getId());
                    Assertions.assertEquals("D", parameters.get(3).getId());
                    Assertions.assertEquals("E", parameters.get(4).getId());
                }
            }
        });
    }

}
