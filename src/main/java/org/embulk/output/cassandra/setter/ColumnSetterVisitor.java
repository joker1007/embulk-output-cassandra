package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

public class ColumnSetterVisitor implements ColumnVisitor {
  private final PageReader pageReader;
  private final CassandraColumnSetter setter;
  private final boolean isPkey;
  private final boolean isDelete;

  private BoundStatementBuilder statementBuilder;

  public ColumnSetterVisitor(
      PageReader pageReader, CassandraColumnSetter setter, boolean isPkey, boolean isDelete) {
    this.pageReader = pageReader;
    this.setter = setter;
    this.isPkey = isPkey;
    this.isDelete = isDelete;
  }

  public boolean hasSetter() {
    return this.setter != null;
  }

  public void setStatementBuilder(BoundStatementBuilder statementBuilder) {
    this.statementBuilder = statementBuilder;
  }

  @Override
  public void booleanColumn(Column column) {
    if (isDelete && !isPkey) {
      return;
    }

    if (pageReader.isNull(column)) {
      setter.setNullValue(statementBuilder);
    } else {
      setter.setBooleanValue(pageReader.getBoolean(column), statementBuilder);
    }
  }

  @Override
  public void longColumn(Column column) {
    if (isDelete && !isPkey) {
      return;
    }

    if (pageReader.isNull(column)) {
      setter.setNullValue(statementBuilder);
    } else {
      setter.setLongValue(pageReader.getLong(column), statementBuilder);
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (isDelete && !isPkey) {
      return;
    }

    if (pageReader.isNull(column)) {
      setter.setNullValue(statementBuilder);
    } else {
      setter.setDoubleValue(pageReader.getDouble(column), statementBuilder);
    }
  }

  @Override
  public void stringColumn(Column column) {
    if (isDelete && !isPkey) {
      return;
    }

    if (pageReader.isNull(column)) {
      setter.setNullValue(statementBuilder);
    } else {
      setter.setStringValue(pageReader.getString(column), statementBuilder);
    }
  }

  @Override
  public void timestampColumn(Column column) {
    if (isDelete && !isPkey) {
      return;
    }

    if (pageReader.isNull(column)) {
      setter.setNullValue(statementBuilder);
    } else {
      setter.setTimestampValue(pageReader.getTimestampInstant(column), statementBuilder);
    }
  }

  @Override
  public void jsonColumn(Column column) {
    if (isDelete && !isPkey) {
      return;
    }

    if (pageReader.isNull(column)) {
      setter.setNullValue(statementBuilder);
    } else {
      setter.setJsonValue(pageReader.getJsonValue(column), statementBuilder);
    }
  }
}
