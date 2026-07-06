/**
 * Copyright (C) 2026 manticore-projects Co. Ltd. <support@manticore-projects.com>
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

class MSqlSplitterTest {

    private static List<String> split(String sql) throws IOException {
        return split(sql, true);
    }

    private static List<String> split(String sql, boolean plsql) throws IOException {
        ArrayList<String> statements = new ArrayList<>();
        try (MSqlSplitter splitter = new MSqlSplitter(new StringReader(sql), plsql)) {
            String statement;
            while ((statement = splitter.nextStatement()) != null) {
                statements.add(statement);
            }
        }
        return statements;
    }

    @Test
    void testSimpleSplit() throws IOException {
        Assertions.assertEquals(List.of("SELECT 1", "SELECT 2"), split("SELECT 1; SELECT 2;"));
        Assertions.assertEquals(List.of("SELECT 1"), split("SELECT 1"));
        Assertions.assertTrue(split("").isEmpty());
        Assertions.assertEquals(List.of("SELECT 1"), split("\uFEFFSELECT 1;"));
    }

    @Test
    void testLiterals() throws IOException {
        List<String> statements = split("INSERT INTO t VALUES ('a;b');SELECT 1");
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).contains("'a;b'"));

        statements = split("SELECT 'It''s; fine' FROM dual;");
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0).contains("It''s; fine"));

        statements = split("SELECT q'[it's; not :param]' FROM dual;");
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0).contains("q'[it's; not :param]'"));

        statements = split("DO $body$ BEGIN PERFORM 1; END $body$;", false);
        Assertions.assertEquals(1, statements.size());
    }

    @Test
    void testComments() throws IOException {
        List<String> statements = split("SELECT 1 -- :fake ; comment\n, 2 FROM dual;");
        Assertions.assertEquals(1, statements.size());

        statements = split("SELECT /*+ FULL(t) */ * FROM t;");
        Assertions.assertTrue(statements.get(0).contains("/*+ FULL(t) */"),
                "optimizer hint must be preserved");

        Assertions.assertEquals(1, split("/* a /* nested ; */ c */ SELECT 1;").size());
        Assertions.assertEquals(List.of("SELECT 1"), split("-- header only\n;\n\nSELECT 1;"));
    }

    @Test
    void testParameterCandidates() throws IOException {
        try (MSqlSplitter splitter = new MSqlSplitter(new StringReader(
                "SELECT :a, :b, :a FROM dual WHERE x = :c AND y = ':fake'"
                        + " /* :fake2 */ AND z::int = 1 AND w = ? AND v := 2;"))) {
            splitter.nextStatement();
            Assertions.assertEquals(new LinkedHashSet<>(List.of("a", "b", "c")),
                    splitter.getNamedParameters());
            Assertions.assertEquals(1, splitter.getPositionalParameterCount());
        }

        try (MSqlSplitter splitter = new MSqlSplitter(
                new StringReader("SELECT q'[:x]' FROM dual WHERE a = :real;"))) {
            splitter.nextStatement();
            Assertions.assertEquals(Set.of("real"), splitter.getNamedParameters());
        }
    }

    @Test
    void testPlSqlTermination() throws IOException {
        List<String> statements =
                split("BEGIN\n  UPDATE t SET x = 1;\n  COMMIT;\nEND;\n/\nSELECT 1;");
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).endsWith("END;"));

        statements = split(
                "CREATE OR REPLACE PROCEDURE p IS\nBEGIN\n  NULL;\nEND p;\n/\nGRANT EXECUTE ON p TO x;");
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).endsWith("END p;"));

        // a sole BEGIN; is transaction control, not a block
        Assertions.assertEquals(List.of("BEGIN", "UPDATE t SET x = 1", "COMMIT"),
                split("BEGIN;\nUPDATE t SET x = 1;\nCOMMIT;"));

        Assertions.assertEquals(List.of("SELECT 1", "SELECT 2"), split("SELECT 1\n/\nSELECT 2\n/"));
        Assertions.assertTrue(split("SELECT 4/2 FROM dual;").get(0).contains("4/2"),
                "division is not a terminator");
    }

    @Test
    void testOracleDollarIdentifiers() throws IOException {
        Assertions.assertEquals(List.of("SELECT * FROM V$SESSION"),
                split("SELECT * FROM V$SESSION;"));
        Assertions.assertEquals(2,
                split("SELECT * FROM SYS.AQ$_QUEUES$ WHERE x = 1; SELECT 2;").size());
    }
}
