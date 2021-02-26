/*
 * Initial Developer: Pavel Ganelin
 */
package org.h2.expression;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;

import java.util.ArrayList;

/**
 * A mathematical expression, or string concatenation.
 */
public class CalculatedAlias extends Expression {

    private String name;
    private Expression alias;

    public CalculatedAlias(String name) {
        this.name = name;
    }

    @Override
    public String getSQL() {
        return "CALCULATED " + name;
    }

    @Override
    public Value getValue(Session session) {
        return alias.getValue(session);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        ArrayList<Expression> expressions = resolver.getSelect().getExpressions();
        for (Expression expression : expressions) {
            if (expression instanceof Alias && expression.getAlias().equals(name) && ((Alias) expression).isMapped()) {
                this.alias = expression;
                return;
            }
        }
        throw DbException.get(ErrorCode.CALCULATED_ALIAS_NOT_FOUND, name);
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
    }

    @Override
    public int getType() {
        return alias.getType();
    }

    @Override
    public long getPrecision() {
        return alias.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return alias.getDisplaySize();
    }

    @Override
    public int getScale() {
        return alias.getScale();
    }

    @Override
    public void updateAggregate(Session session) {
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return true;
    }

    @Override
    public int getCost() {
        return 1;
    }


}
