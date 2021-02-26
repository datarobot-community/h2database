/*
 * Initial Developer: Pavel Ganelin
 */
package org.h2.command.dml;

import org.h2.expression.Expression;

/**
 * Describes one element of the GROUP BY clause of a query.
 */
public class GroupBy {

    /**
     * The order by expression.
     */
    public Expression expression;

    /**
     * The column index expression. This can be a column index number (1 meaning
     * the first column of the select list).
     */
    int columnIndexExpr;

    public GroupBy(Expression expression) {
        this.expression = expression;
    }

    public GroupBy(int index) {
        this.columnIndexExpr = index;
    }

    public String getSQL() {
        StringBuilder buff = new StringBuilder();
        if (expression != null) {
            buff.append('=').append(expression.getSQL());
        } else {
            buff.append(columnIndexExpr);
        }
        return buff.toString();
    }

}
