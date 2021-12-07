package org.opendatadiscovery.tracing.gateway.db;

import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.ConnectByRootOperator;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonAggregateFunction;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.JsonFunctionExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.OracleNamedFunctionParameter;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.RowGetExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.TimezoneExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.VariableAssignment;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SqlExpressionVisitor implements ExpressionVisitor, ItemsListVisitor {
    private final SqlParserVisitor parserVisitor;

    public SqlExpressionVisitor(final SqlParserVisitor parserVisitor) {
        this.parserVisitor = parserVisitor;
    }

    @Override
    public void visit(final BitwiseRightShift shift) {
        visitBinaryExpression(shift);
    }

    @Override
    public void visit(final BitwiseLeftShift shift) {
        visitBinaryExpression(shift);
    }

    @Override
    public void visit(final NullValue nullValue) {
    }

    @Override
    public void visit(final Function function) {
        if (function.getParameters() != null) {
            function.getParameters().accept(this);
        }
        if (function.getKeep() != null) {
            function.getKeep().accept(this);
        }
        if (function.getOrderByElements() != null) {
            for (final OrderByElement orderByElement : function.getOrderByElements()) {
                orderByElement.getExpression().accept(this);
            }
        }
    }

    @Override
    public void visit(final SignedExpression expr) {
        expr.getExpression().accept(this);
    }

    @Override
    public void visit(final JdbcParameter jdbcParameter) {
    }

    @Override
    public void visit(final JdbcNamedParameter jdbcNamedParameter) {
    }

    @Override
    public void visit(final DoubleValue doubleValue) {
    }

    @Override
    public void visit(final LongValue longValue) {
    }

    @Override
    public void visit(final HexValue hexValue) {
    }

    @Override
    public void visit(final DateValue dateValue) {
    }

    @Override
    public void visit(final TimeValue timeValue) {
    }

    @Override
    public void visit(final TimestampValue timestampValue) {
    }

    @Override
    public void visit(final Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(final StringValue stringValue) {
    }

    @Override
    public void visit(final Addition addition) {
        visitBinaryExpression(addition);
    }

    @Override
    public void visit(final Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(final IntegerDivision division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(final Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    @Override
    public void visit(final Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    @Override
    public void visit(final AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    @Override
    public void visit(final OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(final XorExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(final Between between) {
        if (between.getLeftExpression() != null) {
            between.getLeftExpression().accept(this);
        }
        if (between.getBetweenExpressionStart() != null) {
            between.getBetweenExpressionStart().accept(this);
        }
        if (between.getBetweenExpressionEnd() != null) {
            between.getBetweenExpressionEnd().accept(this);
        }
    }

    @Override
    public void visit(final EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(final GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(final GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(final InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        if (inExpression.getRightExpression() != null) {
            inExpression.getRightExpression().accept(this);
        }
        if (inExpression.getRightExpression() != null) {
            inExpression.getRightItemsList().accept(this);
        } else if (inExpression.getRightItemsList() != null) {
            inExpression.getRightItemsList().accept(this);
        }
    }

    @Override
    public void visit(final FullTextSearch fullTextSearch) {
    }

    @Override
    public void visit(final IsNullExpression isNullExpression) {
        if (isNullExpression.getLeftExpression() != null) {
            isNullExpression.getLeftExpression().accept(this);
        }
    }

    @Override
    public void visit(final IsBooleanExpression isBooleanExpression) {
        if (isBooleanExpression.getLeftExpression() != null) {
            isBooleanExpression.getLeftExpression().accept(this);
        }
    }

    @Override
    public void visit(final LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(final MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(final MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(final NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(final Column tableColumn) {
    }

    @Override
    public void visit(final SubSelect subSelect) {
        this.parserVisitor.visit(subSelect);
    }

    @Override
    public void visit(final ExpressionList expressionList) {
        if (expressionList.getExpressions() != null) {
            for (final Expression expression : expressionList.getExpressions()) {
                expression.accept(this);
            }
        }
    }

    @Override
    public void visit(final NamedExpressionList namedExpressionList) {
        if (namedExpressionList.getExpressions() != null) {
            for (final Expression expression : namedExpressionList.getExpressions()) {
                expression.accept(this);
            }
        }
    }

    @Override
    public void visit(final MultiExpressionList multiExprList) {
        if (multiExprList.getExpressionLists() != null) {
            for (final ExpressionList list : multiExprList.getExpressionLists()) {
                visit(list);
            }
        }
    }

    @Override
    public void visit(final CaseExpression expr) {
        if (expr.getSwitchExpression() != null) {
            expr.getSwitchExpression().accept(this);
        }
        for (final Expression x : expr.getWhenClauses()) {
            x.accept(this);
        }
        if (expr.getElseExpression() != null) {
            expr.getElseExpression().accept(this);
        }
    }

    @Override
    public void visit(final WhenClause expr) {
        expr.getWhenExpression().accept(this);
        expr.getThenExpression().accept(this);
    }

    @Override
    public void visit(final ExistsExpression existsExpression) {
        if (existsExpression.getRightExpression() != null) {
            existsExpression.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(final AnyComparisonExpression anyComparisonExpression) {
    }

    @Override
    public void visit(final Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(final Matches matches) {
        visitBinaryExpression(matches);
    }

    @Override
    public void visit(final BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(final BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(final BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(final CastExpression cast) {
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(final Modulo modulo) {
        visitBinaryExpression(modulo);
    }

    @Override
    public void visit(final AnalyticExpression expr) {
        if (expr.getExpression() != null) {
            expr.getExpression().accept(this);
        }
        if (expr.getDefaultValue() != null) {
            expr.getDefaultValue().accept(this);
        }
        if (expr.getOffset() != null) {
            expr.getOffset().accept(this);
        }
        if (expr.getKeep() != null) {
            expr.getKeep().accept(this);
        }
        for (final OrderByElement element : expr.getOrderByElements()) {
            element.getExpression().accept(this);
        }

        if (expr.getWindowElement() != null) {
            expr.getWindowElement().getRange().getStart().getExpression().accept(this);
            expr.getWindowElement().getRange().getEnd().getExpression().accept(this);
            expr.getWindowElement().getOffset().getExpression().accept(this);
        }
    }

    @Override
    public void visit(final ExtractExpression eexpr) {
        eexpr.getExpression().accept(this);
    }

    @Override
    public void visit(final IntervalExpression iexpr) {
    }

    @Override
    public void visit(final OracleHierarchicalExpression oexpr) {
        oexpr.getConnectExpression().accept(this);
        oexpr.getStartExpression().accept(this);
    }

    @Override
    public void visit(final RegExpMatchOperator rexpr) {
        visitBinaryExpression(rexpr);
    }

    @Override
    public void visit(final JsonExpression jsonExpr) {
        jsonExpr.getExpression().accept(this);
    }

    @Override
    public void visit(final JsonOperator jsonExpr) {
        visitBinaryExpression(jsonExpr);
    }

    @Override
    public void visit(final RegExpMySQLOperator regExpMySQLOperator) {
        visitBinaryExpression(regExpMySQLOperator);
    }

    @Override
    public void visit(final UserVariable var) {
    }

    @Override
    public void visit(final NumericBind bind) {
    }

    @Override
    public void visit(final KeepExpression aexpr) {
        for (final OrderByElement element : aexpr.getOrderByElements()) {
            element.getExpression().accept(this);
        }
    }

    @Override
    public void visit(final MySQLGroupConcat groupConcat) {
        for (final Expression expr : groupConcat.getExpressionList().getExpressions()) {
            expr.accept(this);
        }
        if (groupConcat.getOrderByElements() != null) {
            for (final OrderByElement element : groupConcat.getOrderByElements()) {
                element.getExpression().accept(this);
            }
        }
    }

    @Override
    public void visit(final ValueListExpression valueList) {
        for (final Expression expr : valueList.getExpressionList().getExpressions()) {
            expr.accept(this);
        }
    }

    @Override
    public void visit(final RowConstructor rowConstructor) {
        if (rowConstructor.getColumnDefinitions().isEmpty()) {
            for (final Expression expression : rowConstructor.getExprList().getExpressions()) {
                expression.accept(this);
            }
        }
    }

    @Override
    public void visit(final RowGetExpression rowGetExpression) {
        rowGetExpression.getExpression().accept(this);
    }

    @Override
    public void visit(final OracleHint hint) {
    }

    @Override
    public void visit(final TimeKeyExpression timeKeyExpression) {
    }

    @Override
    public void visit(final DateTimeLiteralExpression literal) {
    }

    @Override
    public void visit(final NotExpression exp) {
        exp.getExpression().accept(this);
    }

    @Override
    public void visit(final NextValExpression exp) {
    }

    @Override
    public void visit(final CollateExpression exp) {
        exp.getLeftExpression().accept(this);
    }

    @Override
    public void visit(final SimilarToExpression exp) {
        visitBinaryExpression(exp);
    }

    @Override
    public void visit(final ArrayExpression array) {
        array.getObjExpression().accept(this);
        if (array.getIndexExpression() != null) {
            array.getIndexExpression().accept(this);
        }
        if (array.getStartIndexExpression() != null) {
            array.getStartIndexExpression().accept(this);
        }
        if (array.getStopIndexExpression() != null) {
            array.getStopIndexExpression().accept(this);
        }
    }

    @Override
    public void visit(final ArrayConstructor constructor) {
        for (final Expression expression : constructor.getExpressions()) {
            expression.accept(this);
        }
    }

    @Override
    public void visit(final VariableAssignment assignment) {
        assignment.getVariable().accept(this);
        assignment.getExpression().accept(this);
    }

    @Override
    public void visit(final XMLSerializeExpr expr) {
        expr.getExpression().accept(this);
        for (final OrderByElement elm : expr.getOrderByElements()) {
            elm.getExpression().accept(this);
        }
    }

    @Override
    public void visit(final TimezoneExpression expression) {
        expression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(final JsonAggregateFunction function) {
        Expression expr = function.getExpression();
        if (expr != null) {
            expr.accept(this);
        }

        expr = function.getFilterExpression();
        if (expr != null) {
            expr.accept(this);
        }
    }

    @Override
    public void visit(final JsonFunction function) {
        for (final JsonFunctionExpression expr : function.getExpressions()) {
            expr.getExpression().accept(this);
        }
    }

    @Override
    public void visit(final ConnectByRootOperator operator) {
        operator.getColumn().accept(this);
    }

    @Override
    public void visit(final OracleNamedFunctionParameter parameter) {
        parameter.getExpression().accept(this);
    }

    private void visitBinaryExpression(final BinaryExpression expr) {
        expr.getLeftExpression().accept(this);
        expr.getRightExpression().accept(this);
    }
}
