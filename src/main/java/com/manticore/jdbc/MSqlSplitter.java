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

import java.io.Closeable;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Locale;
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
 * {@code NO_BACKSLASH_ESCAPES} off), and dollar-quote tags longer than {@value #MAX_PUSHBACK}
 * characters.
 */
public final class MSqlSplitter implements Closeable {

    private static final int MAX_PUSHBACK = 128;

    private final PushbackReader reader;
    private final boolean plsqlSlashTermination;

    // --- per-statement state ------------------------------------------------------
    private final StringBuilder sql = new StringBuilder();
    private final LinkedHashSet<String> namedParameters = new LinkedHashSet<>();
    private int positionalParameterCount = 0;
    private boolean hasContent = false;

    private final ArrayList<String> leadingTokens = new ArrayList<>(5);
    private final StringBuilder currentToken = new StringBuilder();
    private Boolean plsqlBlock = null;

    private boolean atLineStart = true;
    private char prevChar = '\n';
    private boolean firstRead = true;

    public MSqlSplitter(Reader reader) {
        this(reader, true);
    }

    public MSqlSplitter(Reader reader, boolean plsqlSlashTermination) {
        this.reader = new PushbackReader(reader, MAX_PUSHBACK);
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
        while ((c = reader.read()) >= 0) {
            if (firstRead) {
                firstRead = false;
                if (c == '\uFEFF') {
                    continue; // byte order mark
                }
            }

            // lone '/' on a line: SQL*Plus "execute buffer" terminator
            if (c == '/' && atLineStart && isSlashTerminatorLine()) {
                if (hasContent) {
                    return emit();
                }
                resetStatementState();
                continue;
            }

            switch (c) {
                case ';':
                    flushToken();
                    if (isPlSqlBlock()) {
                        appendCode(';');
                    } else {
                        prevChar = ';';
                        if (hasContent) {
                            return emit();
                        }
                        resetStatementState(); // empty statement, e.g. ";;"
                    }
                    break;
                case '\'':
                    readSingleQuoted();
                    break;
                case '"':
                    readDoubleQuoted();
                    break;
                case '-':
                    if (peekIs('-')) {
                        readLineComment();
                    } else {
                        appendCode((char) c);
                    }
                    break;
                case '/':
                    if (peekIs('*')) {
                        readBlockComment();
                    } else {
                        appendCode((char) c);
                    }
                    break;
                case ':':
                    readColon();
                    break;
                case '?':
                    positionalParameterCount++;
                    appendCode((char) c);
                    break;
                case '$':
                    if (isIdentifierPart(prevChar) || !tryReadDollarQuoted()) {
                        appendCode((char) c);
                    }
                    break;
                case 'q':
                case 'Q':
                    if (!isIdentifierPart(prevChar) && peekIs('\'')) {
                        readOracleQuoted((char) c);
                    } else {
                        appendCode((char) c);
                    }
                    break;
                default:
                    appendCode((char) c);
            }
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
        return plsqlBlock != null && plsqlBlock;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    // ------------------------------------------------------------------------------

    private void resetStatementState() {
        sql.setLength(0);
        namedParameters.clear();
        positionalParameterCount = 0;
        hasContent = false;
        leadingTokens.clear();
        currentToken.setLength(0);
        plsqlBlock = null;
    }

    private String emit() {
        return sql.toString().trim();
    }

    /** Appends a code character (outside literals and comments), maintaining lexer state. */
    private void appendCode(char c) {
        sql.append(c);
        prevChar = c;
        if (c == '\n' || c == '\r') {
            atLineStart = true;
        } else if (!Character.isWhitespace(c)) {
            atLineStart = false;
            hasContent = true;
        }
        if (isIdentifierPart(c)) {
            if (leadingTokens.size() < 5) {
                currentToken.append(c);
            }
        } else {
            flushToken();
        }
    }

    /** Appends literal or comment content verbatim, tracking line starts only. */
    private void appendRaw(char c) {
        sql.append(c);
        prevChar = c;
        if (c == '\n' || c == '\r') {
            atLineStart = true;
        } else if (!Character.isWhitespace(c)) {
            atLineStart = false;
        }
    }

    private void flushToken() {
        if (currentToken.length() > 0 && leadingTokens.size() < 5) {
            leadingTokens.add(currentToken.toString().toUpperCase(Locale.ROOT));
        }
        currentToken.setLength(0);
    }

    private boolean isPlSqlBlock() {
        if (!plsqlSlashTermination) {
            return false;
        }
        if (plsqlBlock == null) {
            plsqlBlock = decidePlSqlBlock();
        }
        return plsqlBlock;
    }

    private boolean decidePlSqlBlock() {
        if (leadingTokens.isEmpty()) {
            return false;
        }
        String first = leadingTokens.get(0);
        switch (first) {
            case "DECLARE":
                return true;
            case "BEGIN":
                // a sole "BEGIN;" is transaction control, not a block
                return leadingTokens.size() > 1;
            case "CREATE":
                int i = 1;
                if (leadingTokens.size() > 2 && "OR".equals(leadingTokens.get(1))
                        && "REPLACE".equals(leadingTokens.get(2))) {
                    i = 3;
                }
                if (leadingTokens.size() > i) {
                    switch (leadingTokens.get(i)) {
                        case "FUNCTION":
                        case "PROCEDURE":
                        case "PACKAGE":
                        case "TRIGGER":
                        case "TYPE":
                        case "LIBRARY":
                            return true;
                        default:
                            return false;
                    }
                }
                return false;
            default:
                return false;
        }
    }

    /** After a '/' at line start: true when the rest of the line is blank (terminator). */
    private boolean isSlashTerminatorLine() throws IOException {
        int c;
        int consumedWhitespace = 0;
        char[] buffer = new char[MAX_PUSHBACK];
        while ((c = reader.read()) >= 0) {
            if (c == '\n' || c == '\r') {
                return true; // newline consumed together with the terminator line
            }
            if (!Character.isWhitespace((char) c) || consumedWhitespace == buffer.length) {
                reader.unread(c);
                reader.unread(buffer, 0, consumedWhitespace);
                return false;
            }
            buffer[consumedWhitespace++] = (char) c;
        }
        return true; // EOF right after '/'
    }

    private boolean peekIs(char expected) throws IOException {
        int c = reader.read();
        if (c == expected) {
            return true;
        }
        if (c >= 0) {
            reader.unread(c);
        }
        return false;
    }

    private void readSingleQuoted() throws IOException {
        appendRaw('\'');
        hasContent = true;
        int c;
        while ((c = reader.read()) >= 0) {
            appendRaw((char) c);
            if (c == '\'') {
                if (peekIs('\'')) {
                    appendRaw('\''); // '' escape
                } else {
                    break;
                }
            }
        }
    }

    private void readDoubleQuoted() throws IOException {
        appendRaw('"');
        hasContent = true;
        int c;
        while ((c = reader.read()) >= 0) {
            appendRaw((char) c);
            if (c == '"') {
                if (peekIs('"')) {
                    appendRaw('"'); // "" escape
                } else {
                    break;
                }
            }
        }
    }

    /** Oracle alternative quoting: q'X ... X' with paired delimiters for ( ) [ ] { } < >. */
    private void readOracleQuoted(char q) throws IOException {
        // the opening '\'' was already consumed by peekIs
        int delimiter = reader.read();
        if (delimiter < 0) {
            appendCode(q);
            appendRaw('\'');
            return;
        }
        char open = (char) delimiter;
        char close;
        switch (open) {
            case '(':
                close = ')';
                break;
            case '[':
                close = ']';
                break;
            case '{':
                close = '}';
                break;
            case '<':
                close = '>';
                break;
            default:
                close = open;
        }
        appendRaw(q);
        appendRaw('\'');
        appendRaw(open);
        hasContent = true;
        int c;
        while ((c = reader.read()) >= 0) {
            appendRaw((char) c);
            if (c == close && peekIs('\'')) {
                appendRaw('\'');
                break;
            }
        }
    }

    /** PostgreSQL dollar quoting: $tag$ ... $tag$. Returns false when it is not a quote start. */
    private boolean tryReadDollarQuoted() throws IOException {
        char[] buffer = new char[MAX_PUSHBACK];
        int n = 0;
        int c;
        while (n < buffer.length && (c = reader.read()) >= 0) {
            buffer[n++] = (char) c;
            if (c == '$') {
                String tag = "$" + new String(buffer, 0, n); // e.g. "$tag$" or "$$"
                appendRaw('$');
                for (int i = 0; i < n; i++) {
                    appendRaw(buffer[i]);
                }
                hasContent = true;
                readUntilTag(tag);
                return true;
            }
            if (!isIdentifierPart((char) c) || Character.isDigit(buffer[0])) {
                break; // not a valid tag
            }
        }
        reader.unread(buffer, 0, n);
        return false;
    }

    private void readUntilTag(String tag) throws IOException {
        int matched = 0;
        int c;
        while ((c = reader.read()) >= 0) {
            appendRaw((char) c);
            matched = c == tag.charAt(matched) ? matched + 1 : (c == '$' ? 1 : 0);
            if (matched == tag.length()) {
                return;
            }
        }
    }

    private void readLineComment() throws IOException {
        // the second '-' was already consumed by peekIs
        appendRaw('-');
        appendRaw('-');
        int c;
        while ((c = reader.read()) >= 0) {
            appendRaw((char) c);
            if (c == '\n') {
                return;
            }
        }
    }

    /** Block comment with nesting support, preserved verbatim (optimizer hints!). */
    private void readBlockComment() throws IOException {
        // the '*' was already consumed by peekIs
        appendRaw('/');
        appendRaw('*');
        int depth = 1;
        int c;
        while (depth > 0 && (c = reader.read()) >= 0) {
            appendRaw((char) c);
            if (c == '*' && peekIs('/')) {
                appendRaw('/');
                depth--;
            } else if (c == '/' && peekIs('*')) {
                appendRaw('*');
                depth++;
            }
        }
    }

    /** Distinguishes :name parameters from :: casts and := assignments. */
    private void readColon() throws IOException {
        int next = reader.read();
        if (next == ':') {
            appendCode(':');
            appendCode(':');
            return;
        }
        if (next == '=') {
            appendCode(':');
            appendCode('=');
            return;
        }
        if (next < 0 || !isIdentifierPart((char) next)) {
            appendCode(':');
            if (next >= 0) {
                reader.unread(next);
            }
            return;
        }
        StringBuilder name = new StringBuilder();
        name.append((char) next);
        int c;
        while ((c = reader.read()) >= 0 && isIdentifierPart((char) c)) {
            name.append((char) c);
        }
        if (c >= 0) {
            reader.unread(c);
        }
        namedParameters.add(name.toString());
        appendCode(':');
        for (int i = 0; i < name.length(); i++) {
            appendCode(name.charAt(i));
        }
    }

    private static boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
    }
}
