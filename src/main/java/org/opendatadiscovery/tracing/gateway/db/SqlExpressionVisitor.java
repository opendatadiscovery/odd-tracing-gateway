package org.opendatadiscovery.tracing.gateway.db;

import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.ArrayExpression;
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
import net.sf.jsqlparser.statement.select.SubSelect;

public class SqlExpressionVisitor implements ExpressionVisitor, ItemsListVisitor {
    private final SqlParserVisitor parserVisitor;

    public SqlExpressionVisitor(final SqlParserVisitor parserVisitor) {
        this.parserVisitor = parserVisitor;
    }

    @Override
    public void visit(final BitwiseRightShift shift) {
    }

    @Override
    public void visit(final BitwiseLeftShift shift) {
    }

    @Override
    public void visit(final NullValue nullValue) {
    }

    @Override
    public void visit(final Function function) {
    }

    @Override
    public void visit(final SignedExpression signedExpression) {
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
    }

    @Override
    public void visit(final StringValue stringValue) {
    }

    @Override
    public void visit(final Addition addition) {
    }

    @Override
    public void visit(final Division division) {
    }

    @Override
    public void visit(final IntegerDivision division) {
    }

    @Override
    public void visit(final Multiplication multiplication) {
    }

    @Override
    public void visit(final Subtraction subtraction) {
    }

    @Override
    public void visit(final AndExpression andExpression) {
    }

    @Override
    public void visit(final OrExpression orExpression) {
    }

    @Override
    public void visit(final XorExpression orExpression) {
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
        if (equalsTo.getLeftExpression() != null) {
            equalsTo.getLeftExpression().accept(this);
        }
        if (equalsTo.getRightExpression() != null) {
            equalsTo.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(final GreaterThan greaterThan) {
        if (greaterThan.getLeftExpression() != null) {
            greaterThan.getLeftExpression().accept(this);
        }
        if (greaterThan.getRightExpression() != null) {
            greaterThan.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(final GreaterThanEquals greaterThanEquals) {
        if (greaterThanEquals.getLeftExpression() != null) {
            greaterThanEquals.getLeftExpression().accept(this);
        }
        if (greaterThanEquals.getRightExpression() != null) {
            greaterThanEquals.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(final InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        if (inExpression.getRightExpression() != null) {
            inExpression.getRightExpression().accept(this);
        }
        if (inExpression.getRightItemsList() != null) {
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
        if (likeExpression.getLeftExpression() != null) {
            likeExpression.getLeftExpression().accept(this);
        }
        if (likeExpression.getRightExpression() != null) {
            likeExpression.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(final MinorThan minorThan) {
        if (minorThan.getLeftExpression() != null) {
            minorThan.getLeftExpression().accept(this);
        }
        if (minorThan.getRightExpression() != null) {
            minorThan.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(final MinorThanEquals minorThanEquals) {
        if (minorThanEquals.getLeftExpression() != null) {
            minorThanEquals.getLeftExpression().accept(this);
        }
        if (minorThanEquals.getRightExpression() != null) {
            minorThanEquals.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(final NotEqualsTo notEqualsTo) {
        if (notEqualsTo.getLeftExpression() != null) {
            notEqualsTo.getLeftExpression().accept(this);
        }
        if (notEqualsTo.getRightExpression() != null) {
            notEqualsTo.getRightExpression().accept(this);
        }
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
            for (final ExpressionList expressionList : multiExprList.getExpressionLists()) {
                expressionList.accept(this);
            }
        }
    }

    @Override
    public void visit(final CaseExpression caseExpression) {
    }

    @Override
    public void visit(final WhenClause whenClause) {
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
    }

    @Override
    public void visit(final Matches matches) {
    }

    @Override
    public void visit(final BitwiseAnd bitwiseAnd) {
    }

    @Override
    public void visit(final BitwiseOr bitwiseOr) {
    }

    @Override
    public void visit(final BitwiseXor bitwiseXor) {
    }

    @Override
    public void visit(final CastExpression cast) {
    }

    @Override
    public void visit(final Modulo modulo) {
    }

    @Override
    public void visit(final AnalyticExpression aexpr) {
    }

    @Override
    public void visit(final ExtractExpression eexpr) {
    }

    @Override
    public void visit(final IntervalExpression iexpr) {
    }

    @Override
    public void visit(final OracleHierarchicalExpression oexpr) {
    }

    @Override
    public void visit(final RegExpMatchOperator rexpr) {
    }

    @Override
    public void visit(final JsonExpression jsonExpr) {
    }

    @Override
    public void visit(final JsonOperator jsonExpr) {
    }

    @Override
    public void visit(final RegExpMySQLOperator regExpMySQLOperator) {
    }

    @Override
    public void visit(final UserVariable var) {
    }

    @Override
    public void visit(final NumericBind bind) {
    }

    @Override
    public void visit(final KeepExpression aexpr) {
    }

    @Override
    public void visit(final MySQLGroupConcat groupConcat) {
    }

    @Override
    public void visit(final ValueListExpression valueList) {
    }

    @Override
    public void visit(final RowConstructor rowConstructor) {
    }

    @Override
    public void visit(final RowGetExpression rowGetExpression) {
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
    }

    @Override
    public void visit(final NextValExpression exp) {
    }

    @Override
    public void visit(final CollateExpression exp) {
    }

    @Override
    public void visit(final SimilarToExpression exp) {
    }

    @Override
    public void visit(final ArrayExpression exp) {
    }

    @Override
    public void visit(final ArrayConstructor constructor) {
    }

    @Override
    public void visit(final VariableAssignment assignment) {
    }

    @Override
    public void visit(final XMLSerializeExpr expr) {
    }

    @Override
    public void visit(final TimezoneExpression expression) {
    }

    @Override
    public void visit(final JsonAggregateFunction function) {
    }

    @Override
    public void visit(final JsonFunction function) {
    }

    @Override
    public void visit(final ConnectByRootOperator operator) {
    }

    @Override
    public void visit(final OracleNamedFunctionParameter parameter) {
    }
}
