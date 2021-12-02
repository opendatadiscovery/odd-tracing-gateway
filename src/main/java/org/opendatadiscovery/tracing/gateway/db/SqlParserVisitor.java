package org.opendatadiscovery.tracing.gateway.db;

import java.util.HashSet;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.CreateFunctionalStatement;
import net.sf.jsqlparser.statement.DeclareStatement;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.IfElseStatement;
import net.sf.jsqlparser.statement.PurgeStatement;
import net.sf.jsqlparser.statement.ResetStatement;
import net.sf.jsqlparser.statement.RollbackStatement;
import net.sf.jsqlparser.statement.SavepointStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterSession;
import net.sf.jsqlparser.statement.alter.AlterSystemStatement;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.alter.sequence.AlterSequence;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.schema.CreateSchema;
import net.sf.jsqlparser.statement.create.sequence.CreateSequence;
import net.sf.jsqlparser.statement.create.synonym.CreateSynonym;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;

public class SqlParserVisitor implements StatementVisitor, SelectVisitor, FromItemVisitor, SelectItemVisitor {
    private final Set<Table> inputTables = new HashSet<>();
    private final Set<Table> outputTables = new HashSet<>();
    private final Set<String> subQueries = new HashSet<>();

    public Set<Table> getInputTables() {
        return inputTables;
    }

    public Set<Table> getOutputTables() {
        return outputTables;
    }

    @Override
    public void visit(final SavepointStatement savepointStatement) {
    }

    @Override
    public void visit(final RollbackStatement rollbackStatement) {
    }

    @Override
    public void visit(final Comment comment) {
    }

    @Override
    public void visit(final Commit commit) {
    }

    @Override
    public void visit(final Delete delete) {
        if (delete.getWithItemsList() != null) {
            for (final WithItem withItem : delete.getWithItemsList()) {
                withItem.accept(this);
            }
        }
        if (delete.getWhere() != null) {
            final SqlExpressionVisitor expressionVisitor = new SqlExpressionVisitor(this);
            delete.getWhere().accept(expressionVisitor);
        }
        if (delete.getJoins() != null) {
            for (final Join join : delete.getJoins()) {
                if (join.getRightItem() != null) {
                    join.getRightItem().accept(this);
                }
            }
        }
        this.outputTables.add(delete.getTable());
    }

    @Override
    public void visit(final Update update) {
        if (update.getWithItemsList() != null) {
            for (final WithItem withItem : update.getWithItemsList()) {
                withItem.accept(this);
            }
        }
        if (update.getWhere() != null) {
            final SqlExpressionVisitor expressionVisitor = new SqlExpressionVisitor(this);
            update.getWhere().accept(expressionVisitor);
        }
        if (update.getUpdateSets() != null) {
            for (final UpdateSet updateSet : update.getUpdateSets()) {
                if (updateSet.getExpressions() != null) {
                    for (final Expression expression : updateSet.getExpressions()) {
                        final SqlExpressionVisitor expressionVisitor = new SqlExpressionVisitor(this);
                        expression.accept(expressionVisitor);
                    }
                }
            }
        }
        if (update.getJoins() != null) {
            for (final Join join : update.getJoins()) {
                if (join.getRightItem() != null) {
                    join.getRightItem().accept(this);
                }
            }
        }
        this.outputTables.add(update.getTable());
    }

    @Override
    public void visit(final Insert insert) {
        if (insert.getWithItemsList() != null) {
            for (final WithItem withItem : insert.getWithItemsList()) {
                withItem.accept(this);
            }
        }
        this.outputTables.add(insert.getTable());
    }

    @Override
    public void visit(final Replace replace) {
    }

    @Override
    public void visit(final Drop drop) {
    }

    @Override
    public void visit(final Truncate truncate) {
    }

    @Override
    public void visit(final CreateIndex createIndex) {
    }

    @Override
    public void visit(final CreateSchema schema) {
    }

    @Override
    public void visit(final CreateTable createTable) {
    }

    @Override
    public void visit(final CreateView createView) {
    }

    @Override
    public void visit(final AlterView alterView) {
    }

    @Override
    public void visit(final Alter alter) {
    }

    @Override
    public void visit(final Statements stmts) {
    }

    @Override
    public void visit(final Execute execute) {
    }

    @Override
    public void visit(final SetStatement set) {
    }

    @Override
    public void visit(final ResetStatement reset) {
    }

    @Override
    public void visit(final ShowColumnsStatement set) {
    }

    @Override
    public void visit(final ShowTablesStatement showTables) {
    }

    @Override
    public void visit(final Merge merge) {
        this.outputTables.add(merge.getTable());
    }

    @Override
    public void visit(final Select select) {
        select.getSelectBody().accept(this);
        if (select.getWithItemsList() != null) {
            for (final WithItem withItem : select.getWithItemsList()) {
                subQueries.add(withItem.getName());
                inputTables.stream()
                    .filter(s -> s.getFullyQualifiedName().equals(withItem.getName()))
                    .findFirst().ifPresent(inputTables::remove);
                withItem.accept(this);
            }
        }
    }

    @Override
    public void visit(final Upsert upsert) {
        this.outputTables.add(upsert.getTable());
    }

    @Override
    public void visit(final UseStatement use) {
    }

    @Override
    public void visit(final Block block) {
    }

    @Override
    public void visit(final PlainSelect plainSelect) {
        plainSelect.getFromItem().accept(this);
        if (plainSelect.getSelectItems() != null) {
            for (final SelectItem selectItem : plainSelect.getSelectItems()) {
                selectItem.accept(this);
            }
        }
        if (plainSelect.getWhere() != null) {
            final SqlExpressionVisitor expressionVisitor = new SqlExpressionVisitor(this);
            plainSelect.getWhere().accept(expressionVisitor);
        }
        if (plainSelect.getJoins() != null) {
            for (final Join join : plainSelect.getJoins()) {
                join.getRightItem().accept(this);
            }
        }
    }

    @Override
    public void visit(final SetOperationList setOpList) {
        if (setOpList.getSelects() != null) {
            for (final SelectBody select : setOpList.getSelects()) {
                select.accept(this);
            }
        }
    }

    @Override
    public void visit(final WithItem withItem) {
        if (withItem.getWithItemList() != null) {
            for (final SelectItem selectItem : withItem.getWithItemList()) {
                selectItem.accept(this);
            }
        }
        if (withItem.getSubSelect() != null) {
            withItem.getSubSelect().accept(this);
        }
    }

    @Override
    public void visit(final ValuesStatement values) {
    }

    @Override
    public void visit(final DescribeStatement describe) {
    }

    @Override
    public void visit(final ExplainStatement statement) {
    }

    @Override
    public void visit(final ShowStatement statement) {
    }

    @Override
    public void visit(final DeclareStatement statement) {
    }

    @Override
    public void visit(final Grant grant) {
    }

    @Override
    public void visit(final CreateSequence createSequence) {
    }

    @Override
    public void visit(final AlterSequence alterSequence) {
    }

    @Override
    public void visit(final CreateFunctionalStatement createFunctionalStatement) {
    }

    @Override
    public void visit(final CreateSynonym createSynonym) {
    }

    @Override
    public void visit(final AlterSession alterSession) {
    }

    @Override
    public void visit(final IfElseStatement statement) {
    }

    @Override
    public void visit(final RenameTableStatement renameTableStatement) {
    }

    @Override
    public void visit(final PurgeStatement purgeStatement) {
    }

    @Override
    public void visit(final AlterSystemStatement alterSystemStatement) {
    }

    @Override
    public void visit(final Table tableName) {
        final String name = tableName.getFullyQualifiedName();
        if (name != null && !subQueries.contains(name)) {
            this.inputTables.add(tableName);
        }
    }

    @Override
    public void visit(final SubSelect subSelect) {
        if (subSelect.getSelectBody() != null) {
            subSelect.getSelectBody().accept(this);
        }
    }

    @Override
    public void visit(final SubJoin subjoin) {
    }

    @Override
    public void visit(final LateralSubSelect lateralSubSelect) {
    }

    @Override
    public void visit(final ValuesList valuesList) {
    }

    @Override
    public void visit(final TableFunction tableFunction) {
    }

    @Override
    public void visit(final ParenthesisFromItem item) {
    }

    @Override
    public void visit(final AllColumns allColumns) {
    }

    @Override
    public void visit(final AllTableColumns allTableColumns) {
    }

    @Override
    public void visit(final SelectExpressionItem selectExpressionItem) {
        if (selectExpressionItem.getExpression() != null) {
            final Expression expression = selectExpressionItem.getExpression();
            if (expression instanceof SubSelect) {
                this.visit((SubSelect) expression);
            }
        }
    }
}
