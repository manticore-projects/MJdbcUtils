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
import java.io.PushbackReader;
import java.io.Reader;

/**
 * The character source of {@link MSqlSplitter}: a pushback reader, the text buffer of the statement
 * under construction and the scanners for everything that has to be copied verbatim, without any
 * interpretation -- single quoted literals, double quoted identifiers, Oracle {@code q'[...]'}
 * quoting, PostgreSQL {@code $tag$...$tag$} quoting, line comments and (nested) block comments.
 * <p>
 * It also owns the two pieces of lexical position state the splitter needs for its decisions: the
 * previously emitted character and whether the reader sits at the start of a line.
 */
final class SqlScanner implements Closeable {

    /** Pushback capacity; also the upper bound for dollar quote tags and whitespace lookahead. */
    static final int MAX_PUSHBACK = 128;

    private final PushbackReader reader;
    private final StringBuilder text = new StringBuilder();

    private char prevChar = '\n';
    private boolean atLineStart = true;

    SqlScanner(Reader reader) {
        this.reader = new PushbackReader(reader, MAX_PUSHBACK);
    }

    static boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
    }

    int read() throws IOException {
        return reader.read();
    }

    /** Pushes a character back, ignoring the end-of-stream marker. */
    void unread(int c) throws IOException {
        if (c >= 0) {
            reader.unread(c);
        }
    }

    boolean peekIs(char expected) throws IOException {
        int c = reader.read();
        if (c == expected) {
            return true;
        }
        unread(c);
        return false;
    }

    char prevChar() {
        return prevChar;
    }

    boolean isAtLineStart() {
        return atLineStart;
    }

    /** Records a character that terminates a statement and is not part of its text. */
    void noteSeparator(char c) {
        prevChar = c;
    }

    /** The text accumulated since the last {@link #reset()}. */
    String text() {
        return text.toString();
    }

    void reset() {
        text.setLength(0);
    }

    /** Appends literal or comment content verbatim, tracking line starts only. */
    void appendRaw(char c) {
        text.append(c);
        prevChar = c;
        if (c == '\n' || c == '\r') {
            atLineStart = true;
        } else if (!Character.isWhitespace(c)) {
            atLineStart = false;
        }
    }

    /** After a '/' at line start: true when the rest of the line is blank (terminator). */
    boolean isSlashTerminatorLine() throws IOException {
        // one slot of the pushback buffer is reserved for the non-whitespace character below
        char[] buffer = new char[MAX_PUSHBACK - 1];
        int consumedWhitespace = 0;
        int c;
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

    void readSingleQuoted() throws IOException {
        readQuoted('\'');
    }

    void readDoubleQuoted() throws IOException {
        readQuoted('"');
    }

    /** Quoted text with the doubled quote character as the only escape. */
    private void readQuoted(char quote) throws IOException {
        appendRaw(quote);
        int c;
        while ((c = reader.read()) >= 0) {
            appendRaw((char) c);
            if (c == quote) {
                if (peekIs(quote)) {
                    appendRaw(quote); // doubled quote escape
                } else {
                    return;
                }
            }
        }
    }

    /**
     * Oracle alternative quoting: q'X ... X' with paired delimiters for ( ) [ ] { } and &lt; &gt;.
     */
    void readOracleQuoted(char q) throws IOException {
        // the opening '\'' was already consumed by peekIs
        int delimiter = reader.read();
        if (delimiter < 0) {
            appendRaw(q);
            appendRaw('\'');
            return;
        }
        char open = (char) delimiter;
        char close = closingDelimiter(open);

        appendRaw(q);
        appendRaw('\'');
        appendRaw(open);
        int c;
        while ((c = reader.read()) >= 0) {
            appendRaw((char) c);
            if (c == close && peekIs('\'')) {
                appendRaw('\'');
                return;
            }
        }
    }

    private static char closingDelimiter(char open) {
        switch (open) {
            case '(':
                return ')';
            case '[':
                return ']';
            case '{':
                return '}';
            case '<':
                return '>';
            default:
                return open;
        }
    }

    /** PostgreSQL dollar quoting: $tag$ ... $tag$. Returns false when it is not a quote start. */
    boolean tryReadDollarQuoted() throws IOException {
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
            matched = c == tag.charAt(matched) ? matched + 1 : c == '$' ? 1 : 0;
            if (matched == tag.length()) {
                return;
            }
        }
    }

    void readLineComment() throws IOException {
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
    void readBlockComment() throws IOException {
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

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
