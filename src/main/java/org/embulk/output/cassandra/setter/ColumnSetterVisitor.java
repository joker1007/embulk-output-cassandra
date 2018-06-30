package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

public class ColumnSetterVisitor implements ColumnVisitor
{
    private final PageReader pageReader;
    private final CassandraColumnSetter setter;

    private BoundStatement statement;

    public ColumnSetterVisitor(PageReader pageReader, CassandraColumnSetter setter)
    {
        this.pageReader = pageReader;
        this.setter = setter;
    }

    public void setStatement(BoundStatement statement)
    {
        this.statement = statement;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setter.setNullValue(statement);
        }
        else {
            setter.setBooleanValue(pageReader.getBoolean(column), statement);
        }
    }

    @Override
    public void longColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setter.setNullValue(statement);
        }
        else {
            setter.setLongValue(pageReader.getLong(column), statement);
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setter.setNullValue(statement);
        }
        else {
            setter.setDoubleValue(pageReader.getDouble(column), statement);
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setter.setNullValue(statement);
        }
        else {
            setter.setStringValue(pageReader.getString(column), statement);
        }
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setter.setNullValue(statement);
        }
        else {
            setter.setTimestampValue(pageReader.getTimestamp(column), statement);
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setter.setNullValue(statement);
        }
        else {
            setter.setJsonValue(pageReader.getJson(column), statement);
        }
    }
}
