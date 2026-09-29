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

    /** Splits and records, per statement, whether the splitter classified it as a PL/SQL block. */
    private static List<Boolean> plsqlFlags(String sql, boolean plsql) throws IOException {
        ArrayList<Boolean> flags = new ArrayList<>();
        try (MSqlSplitter splitter = new MSqlSplitter(new StringReader(sql), plsql)) {
            while (splitter.nextStatement() != null) {
                flags.add(splitter.isPlSqlStatement());
            }
        }
        return flags;
    }

    // --- Oracle ---------------------------------------------------------------------

    @Test
    void testOracleAnonymousBlocksWithExecuteImmediate() throws IOException {
        String sql = "begin\n"
                             + "  for tables in (select owner, table_name from all_tables"
                             + " where lower(owner) = 'common') loop\n"
                             + "    execute immediate\n"
                             + "      'grant select on \"'||tables.owner||'\".\"'||tables.table_name||'\" to cfe';\n"
                             + "    execute immediate\n"
                             + "      'grant delete on \"'||tables.owner||'\".\"'||tables.table_name||'\" to cfe';\n"
                             + "  end loop;\n"
                             + "end;\n"
                             + "/\n"
                             + "\n"
                             + "begin\n"
                             + "  for tables in (select sequence_owner, sequence_name from all_sequences"
                             + " where lower(sequence_owner) = 'common') loop\n"
                             + "    execute immediate\n"
                             + "      'grant select on \"'||tables.sequence_owner||'\".\"'"
                             + "||tables.sequence_name||'\" to cfe';\n"
                             + "  end loop;\n"
                             + "end;\n"
                             + "/\n";

        List<String> statements = split(sql);
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).startsWith("begin"));
        Assertions.assertTrue(statements.get(0).endsWith("end;"), "block keeps its final ';'");
        Assertions.assertTrue(statements.get(0).contains("end loop;"));
        Assertions.assertTrue(statements.get(1).contains("all_sequences"));
        Assertions.assertTrue(statements.get(1).endsWith("end;"));
        Assertions.assertEquals(List.of(true, true), plsqlFlags(sql, true));
    }

    @Test
    void testOracleDeclareBlock() throws IOException {
        String sql = "DECLARE\n  v NUMBER := 0;\nBEGIN\n  v := v + 1;\n"
                             + "  DBMS_OUTPUT.PUT_LINE('v=' || v);\nEND;\n/\nSELECT 1 FROM dual;";
        try (MSqlSplitter splitter = new MSqlSplitter(new StringReader(sql))) {
            String block = splitter.nextStatement();
            Assertions.assertTrue(block.startsWith("DECLARE") && block.endsWith("END;"));
            Assertions.assertTrue(splitter.isPlSqlStatement());
            Assertions.assertTrue(splitter.getNamedParameters().isEmpty(),
                                  ":= is an assignment, not a parameter");

            Assertions.assertEquals("SELECT 1 FROM dual", splitter.nextStatement());
            Assertions.assertFalse(splitter.isPlSqlStatement());
            Assertions.assertEquals(null, splitter.nextStatement());
        }
    }

    @Test
    void testOraclePackageAndBody() throws IOException {
        String sql = "CREATE OR REPLACE PACKAGE pkg AS\n"
                             + "  PROCEDURE p;\n  FUNCTION f RETURN NUMBER;\nEND pkg;\n/\n"
                             + "CREATE OR REPLACE PACKAGE BODY pkg AS\n"
                             + "  PROCEDURE p IS BEGIN NULL; END;\n"
                             + "  FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nEND pkg;\n/\n";
        List<String> statements = split(sql);
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).endsWith("END pkg;"));
        Assertions.assertTrue(statements.get(1).startsWith("CREATE OR REPLACE PACKAGE BODY"));
        Assertions.assertEquals(List.of(true, true), plsqlFlags(sql, true));
    }

    @Test
    void testOraclePlSqlObjectKinds() throws IOException {
        String sql = "CREATE FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\n/\n"
                             + "CREATE PROCEDURE p IS BEGIN NULL; END;\n/\n"
                             + "CREATE OR REPLACE TYPE t_obj AS OBJECT (a NUMBER, b VARCHAR2(10));\n/\n"
                             + "CREATE OR REPLACE TYPE t_tab AS TABLE OF t_obj;\n/\n"
                             + "CREATE TABLE t (a NUMBER);\n"
                             + "CREATE INDEX t_ix ON t (a);\n"
                             + "CREATE OR REPLACE VIEW v AS SELECT a FROM t;\n";
        Assertions.assertEquals(List.of(true, true, true, true, false, false, false),
                                plsqlFlags(sql, true));
    }

    @Test
    void testOracleTriggerCorrelationNames() throws IOException {
        String sql = "CREATE OR REPLACE TRIGGER trg BEFORE UPDATE ON t FOR EACH ROW\n"
                             + "BEGIN\n  :NEW.changed := SYSDATE;\n  IF :OLD.a <> :NEW.a THEN NULL; END IF;\n"
                             + "END;\n/\n";
        try (MSqlSplitter splitter = new MSqlSplitter(new StringReader(sql))) {
            String trigger = splitter.nextStatement();
            Assertions.assertTrue(trigger.endsWith("END;"));
            Assertions.assertTrue(splitter.isPlSqlStatement());
            // the lexer reports them; MJdbcTools.scanParameters() filters them for PL/SQL
            Assertions.assertEquals(new LinkedHashSet<>(List.of("NEW", "OLD")),
                                    splitter.getNamedParameters());
        }
    }

    @Test
    void testOracleBindParametersInBlock() throws IOException {
        try (MSqlSplitter splitter = new MSqlSplitter(new StringReader(
                "BEGIN\n  UPDATE t SET a = :value WHERE id = :id;\n  -- :ignored\n"
                        + "  x := 'not :a param';\nEND;\n/"))) {
            splitter.nextStatement();
            Assertions.assertEquals(new LinkedHashSet<>(List.of("value", "id")),
                                    splitter.getNamedParameters());
        }
    }

    @Test
    void testOracleAlternativeQuoting() throws IOException {
        Assertions.assertEquals(1, split("SELECT q'{a; b}' FROM dual;").size());
        Assertions.assertEquals(1, split("SELECT q'(a; b)' FROM dual;").size());
        Assertions.assertEquals(1, split("SELECT q'<a; b>' FROM dual;").size());
        Assertions.assertEquals(1, split("SELECT Q'!a; 'b'!' FROM dual;").size());
        Assertions.assertEquals(1, split("SELECT nq'[a; b]' FROM dual;").size(),
                                "national q-quote");

        // q' inside a block, containing a quote and a terminator-like line
        List<String> statements =
                split("BEGIN\n  x := q'[it's;\n/\nstill quoted]';\nEND;\n/\nSELECT 1 FROM dual;");
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).contains("still quoted]'"));
    }

    @Test
    void testOracleIdentifiers() throws IOException {
        Assertions.assertEquals(List.of("SELECT * FROM GV$SESSION s, SYS.OBJ# o",
                                        "SELECT \"a;b\" FROM \"My;Table\""),
                                split("SELECT * FROM GV$SESSION s, SYS.OBJ# o; SELECT \"a;b\" FROM \"My;Table\";"));
        // "q" at the end of an identifier followed by a literal is not a q-quote
        Assertions.assertEquals(2, split("SELECT seq'x;y' FROM dual; SELECT 2;").size());
    }

    @Test
    void testOracleSlashTerminatorVariants() throws IOException {
        Assertions.assertEquals(2, split("BEGIN NULL; END;\n/   \nSELECT 1 FROM dual;").size(),
                                "trailing whitespace after '/'");
        Assertions.assertEquals(2, split("BEGIN NULL; END;\r\n/\r\nSELECT 1 FROM dual;").size(),
                                "CRLF line endings");
        Assertions.assertEquals(List.of("BEGIN NULL; END;"), split("BEGIN NULL; END;\n/"),
                                "'/' at EOF without newline");
        Assertions.assertEquals(List.of("BEGIN NULL; END;"), split("BEGIN NULL; END;\n"),
                                "block terminated by EOF");
        Assertions.assertEquals(1, split("BEGIN\n  x := 10\n  / 2;\nEND;\n/").size(),
                                "'/' followed by more text on the line is division");
        Assertions.assertEquals(List.of("BEGIN NULL; END;", "SELECT 1 FROM dual"),
                                split("BEGIN NULL; END;\n/\n/\nSELECT 1 FROM dual;"),
                                "repeated '/' does not produce empty statements");
    }

    @Test
    void testOracleHintsAndCommentsInBlocks() throws IOException {
        List<String> statements = split("BEGIN\n  -- step 1; do it\n"
                                                + "  INSERT /*+ APPEND */ INTO t SELECT * FROM s;\n  /* ; */ COMMIT;\nEND;\n/");
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0).contains("/*+ APPEND */"));
        Assertions.assertTrue(statements.get(0).contains("-- step 1; do it"));
    }

    // --- PostgreSQL -----------------------------------------------------------------

    @Test
    void testPostgresDoBlock() throws IOException {
        List<String> statements = split("DO $$\nBEGIN\n  PERFORM 1;\n  RAISE NOTICE 'x;y';\n"
                                                + "END\n$$;\nSELECT 1;", false);
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).endsWith("$$"));
        Assertions.assertEquals("SELECT 1", statements.get(1));
    }

    @Test
    void testPostgresFunctionWithDollarQuotes() throws IOException {
        String sql = "CREATE OR REPLACE FUNCTION add_one(i integer) RETURNS integer AS $fn$\n"
                             + "BEGIN\n  RETURN i + 1;\nEND;\n$fn$ LANGUAGE plpgsql;\n"
                             + "SELECT add_one(1);";
        List<String> statements = split(sql, false);
        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).endsWith("LANGUAGE plpgsql"));
        Assertions.assertEquals("SELECT add_one(1)", statements.get(1));
        Assertions.assertEquals(List.of(false, false), plsqlFlags(sql, false));
    }

    @Test
    void testPostgresNestedAndTaggedDollarQuotes() throws IOException {
        List<String> statements = split("SELECT $outer$ a; $inner$ b; $inner$ c; $outer$;"
                                                + " SELECT $$ it's ; $not_the_end $$; SELECT 3;", false);
        Assertions.assertEquals(3, statements.size());
        Assertions.assertTrue(statements.get(0).endsWith("$outer$"));
        Assertions.assertTrue(statements.get(1).contains("$not_the_end"));
    }

    @Test
    void testPostgresPositionalDollarParameters() throws IOException {
        // $1, $2 are not dollar quote tags
        Assertions.assertEquals(List.of("SELECT $1 + $2", "SELECT 2"),
                                split("SELECT $1 + $2; SELECT 2;", false));
        Assertions.assertEquals(
                List.of("PREPARE q(int) AS SELECT * FROM t WHERE a = $1", "EXECUTE q(1)"),
                split("PREPARE q(int) AS SELECT * FROM t WHERE a = $1; EXECUTE q(1);", false));
    }

    @Test
    void testPostgresCastsAreNotParameters() throws IOException {
        try (MSqlSplitter splitter = new MSqlSplitter(new StringReader(
                "SELECT '1'::int, now()::date, a::text[] FROM t WHERE b = :b;"), false)) {
            Assertions.assertEquals("SELECT '1'::int, now()::date, a::text[] FROM t WHERE b = :b",
                                    splitter.nextStatement());
            Assertions.assertEquals(Set.of("b"), splitter.getNamedParameters());
        }
    }

    @Test
    void testPostgresTransactionControl() throws IOException {
        // with PL/SQL slash termination enabled (the default), transaction control must
        // still be split on ';'
        for (String begin : List.of("BEGIN", "BEGIN WORK", "BEGIN TRANSACTION",
                                    "BEGIN ISOLATION LEVEL SERIALIZABLE", "START TRANSACTION")) {
            Assertions.assertEquals(List.of(begin, "UPDATE t SET a = 1", "COMMIT"),
                                    split(begin + ";\nUPDATE t SET a = 1;\nCOMMIT;"), begin);
        }
        Assertions.assertEquals(List.of("BEGIN TRAN", "UPDATE t SET a = 1", "COMMIT TRAN"),
                                split("BEGIN TRAN;\nUPDATE t SET a = 1;\nCOMMIT TRAN;"), "SQL Server");
    }

    @Test
    void testPostgresQuotedIdentifiersAndComments() throws IOException {
        List<String> statements = split("CREATE TABLE \"my;table\" (\"a;\" int); -- trailing; comment\n"
                                                + "/* block ; /* nested ; */ still ; */ SELECT 1;", false);
        Assertions.assertEquals(2, statements.size());
        Assertions.assertEquals("CREATE TABLE \"my;table\" (\"a;\" int)", statements.get(0));
    }

    @Test
    void testPostgresDialectFlagDisablesPlSqlTermination() throws IOException {
        // Oracle-style keywords open ordinary ';'-terminated statements without the flag
        Assertions.assertEquals(List.of("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
                                        "SELECT f()"),
                                split("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1';\nSELECT f();",
                                      false));
        Assertions.assertEquals(List.of(false, false),
                                plsqlFlags("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1';\n"
                                                   + "SELECT f();", false));
    }
}