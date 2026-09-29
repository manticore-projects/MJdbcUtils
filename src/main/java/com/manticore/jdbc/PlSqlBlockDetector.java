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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Collects the leading identifier tokens of a statement and decides -- once, then cached -- whether
 * the statement is a PL/SQL block, i.e. whether {@code ;} is an inner separator rather than the
 * statement terminator.
 */
final class PlSqlBlockDetector {

    /** No decision needs more than the first five tokens ("CREATE OR REPLACE PACKAGE BODY"). */
    private static final int MAX_LEADING_TOKENS = 5;

    private final List<String> leadingTokens = new ArrayList<>(MAX_LEADING_TOKENS);
    private final StringBuilder currentToken = new StringBuilder();
    private Boolean plsqlBlock;

    void reset() {
        leadingTokens.clear();
        currentToken.setLength(0);
        plsqlBlock = null;
    }

    /** Feeds a code character (outside literals and comments) into the token accumulator. */
    void accept(char c) {
        if (SqlScanner.isIdentifierPart(c)) {
            if (leadingTokens.size() < MAX_LEADING_TOKENS) {
                currentToken.append(c);
            }
        } else {
            flushToken();
        }
    }

    void flushToken() {
        if (currentToken.length() > 0 && leadingTokens.size() < MAX_LEADING_TOKENS) {
            leadingTokens.add(currentToken.toString().toUpperCase(Locale.ROOT));
        }
        currentToken.setLength(0);
    }

    /** Decides on first call and caches the answer for the rest of the statement. */
    boolean isPlSqlBlock() {
        if (plsqlBlock == null) {
            plsqlBlock = decide();
        }
        return plsqlBlock;
    }

    /** True only when {@link #isPlSqlBlock()} has been called and answered positively. */
    boolean isDecidedPlSqlBlock() {
        return plsqlBlock != null && plsqlBlock;
    }

    private boolean decide() {
        if (leadingTokens.isEmpty()) {
            return false;
        }
        switch (leadingTokens.get(0)) {
            case "DECLARE":
                return true;
            case "BEGIN":
                // a sole "BEGIN;" and "BEGIN TRANSACTION|WORK|..." are transaction control
                return leadingTokens.size() > 1 && !isTransactionKeyword(leadingTokens.get(1));
            case "CREATE":
                return createsPlSqlObject();
            default:
                return false;
        }
    }

    private boolean createsPlSqlObject() {
        int i = leadingTokens.size() > 2
                        && "OR".equals(leadingTokens.get(1))
                        && "REPLACE".equals(leadingTokens.get(2)) ? 3 : 1;

        return leadingTokens.size() > i && isPlSqlObjectKeyword(leadingTokens.get(i));
    }

    /** PostgreSQL, SQL Server and H2 forms of "BEGIN TRANSACTION". */
    private static boolean isTransactionKeyword(String token) {
        switch (token) {
            case "TRAN":
            case "TRANSACTION":
            case "WORK":
            case "ISOLATION":
            case "DISTRIBUTED":
                return true;
            default:
                return false;
        }
    }

    private static boolean isPlSqlObjectKeyword(String token) {
        switch (token) {
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
}