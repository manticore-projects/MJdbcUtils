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

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A streaming, O(1)-memory SQL statement splitter for arbitrarily large scripts.
 * <p>
 * The splitter is a lexer, not a parser: it understands single-quoted literals (with {@code ''}
 * escapes), double-quoted identifiers, line comments, (nested) block comments, Oracle
 * {@code q'[...]'} quoting and PostgreSQL {@code $tag$...$tag$} quoting, and splits on {@code ;}
 * outside of all of these. Comments -- including optimizer hints -- are preserved verbatim in the
 * emitted statement text.
 * <p>
 * With {@link #MSqlSplitter(Reader, boolean) plsqlSlashTermination} enabled (the default), SQL*Plus
 * semantics apply: statements opening with {@code DECLARE}, {@code BEGIN} (followed by further
 * tokens; a sole {@code BEGIN;} remains transaction control) or
 * {@code CREATE [OR REPLACE] FUNCTION|PROCEDURE|PACKAGE|TRIGGER|TYPE|LIBRARY} are terminated only
 * by a {@code /} alone on a line (or EOF), never by {@code ;}. Additionally, a lone {@code /} line
 * terminates any pending statement, matching SQL*Plus buffer execution. Disable the flag for
 * dialects where these leading keywords introduce ordinary {@code ;}-terminated statements.
 * <p>
 * While splitting, the lexer collects <i>parameter candidates</i> per statement: named parameters
 * ({@code :name}, excluding {@code ::} casts and {@code :=} assignments) and positional ({@code ?})
 * markers, both guaranteed to lie outside literals and comments. These are available from
 * {@link #getNamedParameters()} and {@link #getPositionalParameterCount()} after each
 * {@link #nextStatement()} and are intended to be confirmed by a proper JSqlParser parse where
 * grammar coverage allows -- see {@code MJdbcTools.scanParameters(Reader)}.
 * <p>
 * Not supported (by design, out of lexer scope): SQL*Plus client directives ({@code SET},
 * {@code PROMPT}, {@code REM}, {@code @}), backslash escapes in literals (MySQL
 * {@code NO_BACKSLASH_ESCAPES} off), and dollar-quote tags longer than 128 characters.
 */
public final class MSqlSplitter implements Closeable {

    private final SqlScanner scanner;
    private final PlSqlBlockDetector detector = new PlSqlBlockDetector();
    private final boolean plsqlSlashTermination;

    // --- per-statement state ------------------------------------------------------
    private final LinkedHashSet<String> namedParameters = new LinkedHashSet<>();
    private int positionalParameterCount;
    private boolean hasContent;

    private boolean firstRead = true;

    public MSqlSplitter(Reader reader) {
        this(reader, true);
    }

    public MSqlSplitter(Reader reader, boolean plsqlSlashTermination) {
        this.scanner = new SqlScanner(reader);
        this.plsqlSlashTermination = plsqlSlashTermination;
    }

    /**
     * Returns the next statement (terminators excluded, surrounding whitespace trimmed, comments
     * preserved), skipping chunks that contain only whitespace and comments. Returns {@code null}
     * at the end of the stream.
     */
    public String nextStatement() throws IOException {
        resetStatementState();

        int c;
        while ((c = scanner.read()) >= 0) {
            if (skipByteOrderMark(c)) {
                continue;
            }
            if (endsStatement(c)) {
                if (hasContent) {
                    return emit();
                }
                resetStatementState(); // empty chunk, e.g. ";;" or a stray '/' line
                continue;
            }
            consume(c);
        }

        return hasContent ? emit() : null;
    }

    /** Named parameter candidates of the statement last returned by {@link #nextStatement()}. */
    public Set<String> getNamedParameters() {
        return new LinkedHashSet<>(namedParameters);
    }

    /**
     * Positional {@code ?} candidates of the statement last returned by {@link #nextStatement()}.
     */
    public int getPositionalParameterCount() {
        return positionalParameterCount;
    }

    /** True when the statement last returned by {@link #nextStatement()} was a PL/SQL block. */
    public boolean isPlSqlStatement() {
        return detector.isDecidedPlSqlBlock();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    // ------------------------------------------------------------------------------

    private boolean skipByteOrderMark(int c) {
        if (!firstRead) {
            return false;
        }
        firstRead = false;
        return c == '\uFEFF';
    }

    /** True when the character terminates the statement and is not part of its text. */
    private boolean endsStatement(int c) throws IOException {
        if (c == ';') {
            detector.flushToken();
            if (isPlSqlBlock()) {
                return false; // inner separator of a block; consume() appends it
            }
            scanner.noteSeparator(';');
            return true;
        }
        // a lone '/' on a line is the SQL*Plus "execute buffer" terminator
        return c == '/' && scanner.isAtLineStart() && scanner.isSlashTerminatorLine();
    }

    /** Dispatches a character that belongs to the statement text. */
    private void consume(int c) throws IOException {
        if (consumeComment(c) || consumeQuoted(c)) {
            return;
        }
        if (c == ':') {
            readColon();
            return;
        }
        if (c == '?') {
            positionalParameterCount++;
        }
        appendCode((char) c);
    }

    /** Line and block comments: copied verbatim, never counting as statement content. */
    private boolean consumeComment(int c) throws IOException {
        if (c == '-' && scanner.peekIs('-')) {
            scanner.readLineComment();
            return true;
        }
        if (c == '/' && scanner.peekIs('*')) {
            scanner.readBlockComment();
            return true;
        }
        return false;
    }

    /** Quoted text in all supported flavours: copied verbatim, counting as statement content. */
    private boolean consumeQuoted(int c) throws IOException {
        if (c == '\'') {
            scanner.readSingleQuoted();
        } else if (c == '"') {
            scanner.readDoubleQuoted();
        } else if (c == '$' || c == 'q' || c == 'Q') {
            return consumeDialectQuoted(c);
        } else {
            return false;
        }
        hasContent = true;
        return true;
    }

    /**
     * Oracle {@code q'X..X'} and PostgreSQL {@code $tag$..$tag$} quoting, both needing lookahead.
     */
    private boolean consumeDialectQuoted(int c) throws IOException {
        if (SqlScanner.isIdentifierPart(scanner.prevChar())) {
            return false; // inside an identifier such as V$SESSION, AQ$_QUEUES$ or MAX_Q
        }
        if (c == '$') {
            if (!scanner.tryReadDollarQuoted()) {
                return false;
            }
        } else if (scanner.peekIs('\'')) {
            scanner.readOracleQuoted((char) c);
        } else {
            return false;
        }
        hasContent = true;
        return true;
    }

    private void resetStatementState() {
        scanner.reset();
        namedParameters.clear();
        positionalParameterCount = 0;
        hasContent = false;
        detector.reset();
    }

    private String emit() {
        detector.flushToken();
        // force the decision so that isPlSqlStatement() is also accurate for blocks
        // that were terminated by '/' or EOF without ever containing a ';'
        isPlSqlBlock();
        return scanner.text().trim();
    }

    private boolean isPlSqlBlock() {
        return plsqlSlashTermination && detector.isPlSqlBlock();
    }

    /** Appends a code character (outside literals and comments), maintaining lexer state. */
    private void appendCode(char c) {
        scanner.appendRaw(c);
        if (!Character.isWhitespace(c)) {
            hasContent = true;
        }
        detector.accept(c);
    }

    /** Distinguishes :name parameters from :: casts and := assignments. */
    private void readColon() throws IOException {
        int next = scanner.read();
        if (next == ':' || next == '=') {
            appendCode(':');
            appendCode((char) next);
            return;
        }
        if (next < 0 || !SqlScanner.isIdentifierPart((char) next)) {
            appendCode(':');
            scanner.unread(next);
            return;
        }
        appendNamedParameter(readIdentifier((char) next));
    }

    private String readIdentifier(char first) throws IOException {
        StringBuilder name = new StringBuilder().append(first);
        int c;
        while ((c = scanner.read()) >= 0 && SqlScanner.isIdentifierPart((char) c)) {
            name.append((char) c);
        }
        scanner.unread(c);
        return name.toString();
    }

    private void appendNamedParameter(String name) {
        namedParameters.add(name);
        appendCode(':');
        for (int i = 0; i < name.length(); i++) {
            appendCode(name.charAt(i));
        }
    }
}
