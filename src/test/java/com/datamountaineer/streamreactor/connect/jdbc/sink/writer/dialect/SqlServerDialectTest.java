package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.datamountaineer.streamreactor.connect.jdbc.sink.Field;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlServerDialectTest {
  private final DbDialect dialect = new SqlServerDialect();

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTableIsNull() {
    dialect.getUpsertQuery(null, Lists.newArrayList("value"), Lists.newArrayList("id"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTableNameIsEmptyString() {
    dialect.getUpsertQuery("  ", Lists.newArrayList("value"), Lists.newArrayList("id"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfKeyColsIsNull() {
    dialect.getUpsertQuery("Person", Lists.newArrayList("value"), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfKeyColsIsNullIsEmpty() {
    dialect.getUpsertQuery("Customer", Lists.newArrayList("value"), Lists.<String>newArrayList());
  }

  @Test
  public void produceTheRightSqlStatementWhithASinglePK() {
    String insert = dialect.getUpsertQuery("Customer", Lists.newArrayList("name", "salary", "address"), Lists.newArrayList("id"));
    assertEquals(insert, "merge into Customer with (HOLDLOCK) using (select ? name, ? salary, ? address, ? id) incoming on(Customer.id=incoming.id) " +
            "when matched then update set Customer.name=incoming.name,Customer.salary=incoming.salary,Customer.address=incoming.address" +
            " when not matched then insert(Customer.name,Customer.salary,Customer.address,Customer.id) values(incoming.name,incoming.salary,incoming.address,incoming.id)");

  }

  @Test
  public void produceTheRightSqlStatementWhithACompositePK() {
    String insert = dialect.getUpsertQuery("Book", Lists.newArrayList("ISBN", "year", "pages"), Lists.newArrayList("author", "title"));
    assertEquals(insert, "merge into Book with (HOLDLOCK) using (select ? ISBN, ? year, ? pages, ? author, ? title) incoming on(Book.author=incoming.author and Book.title=incoming.title) " +
            "when matched then update set Book.ISBN=incoming.ISBN,Book.year=incoming.year,Book.pages=incoming.pages" +
            " when not matched then insert(Book.ISBN,Book.year,Book.pages,Book.author,Book.title) values(incoming.ISBN,incoming.year,incoming.pages,incoming.author,incoming.title)");

  }


  @Test
  public void handleCreateTableMultiplePKColumns() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new Field(Schema.Type.INT32, "userid", true),
            new Field(Schema.Type.INT32, "userdataid", true),
            new Field(Schema.Type.STRING, "info", false)
    ));

    String expected = "CREATE TABLE tableA (" + System.lineSeparator() +
            "userid int NOT NULL," + System.lineSeparator() +
            "userdataid int NOT NULL," + System.lineSeparator() +
            "info varchar(256) NULL," + System.lineSeparator() +
            "PRIMARY KEY(userid,userdataid));";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableOnePKColumn() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new Field(Schema.Type.INT32, "col1", true),
            new Field(Schema.Type.INT64, "col2", false),
            new Field(Schema.Type.STRING, "col3", false),
            new Field(Schema.Type.FLOAT32, "col4", false),
            new Field(Schema.Type.FLOAT64, "col5", false),
            new Field(Schema.Type.BOOLEAN, "col6", false),
            new Field(Schema.Type.INT8, "col7", false),
            new Field(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE tableA (" + System.lineSeparator() +
            "col1 int NOT NULL," + System.lineSeparator() +
            "col2 bigint NULL," + System.lineSeparator() +
            "col3 varchar(256) NULL," + System.lineSeparator() +
            "col4 real NULL," + System.lineSeparator() +
            "col5 float NULL," + System.lineSeparator() +
            "col6 bit NULL," + System.lineSeparator() +
            "col7 tinyint NULL," + System.lineSeparator() +
            "col8 smallint NULL," + System.lineSeparator() +
            "PRIMARY KEY(col1));";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableNoPKColumn() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new Field(Schema.Type.INT32, "col1", false),
            new Field(Schema.Type.INT64, "col2", false),
            new Field(Schema.Type.STRING, "col3", false),
            new Field(Schema.Type.FLOAT32, "col4", false),
            new Field(Schema.Type.FLOAT64, "col5", false),
            new Field(Schema.Type.BOOLEAN, "col6", false),
            new Field(Schema.Type.INT8, "col7", false),
            new Field(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE tableA (" + System.lineSeparator() +
            "col1 int NULL," + System.lineSeparator() +
            "col2 bigint NULL," + System.lineSeparator() +
            "col3 varchar(256) NULL," + System.lineSeparator() +
            "col4 real NULL," + System.lineSeparator() +
            "col5 float NULL," + System.lineSeparator() +
            "col6 bit NULL," + System.lineSeparator() +
            "col7 tinyint NULL," + System.lineSeparator() +
            "col8 smallint NULL);";
    assertEquals(expected, actual);
  }

  @Test
  public void handleAmendAddColumns() {
    List<String> actual = dialect.getAlterTable("tableA", Lists.newArrayList(
            new Field(Schema.Type.INT32, "col1", false),
            new Field(Schema.Type.INT64, "col2", false),
            new Field(Schema.Type.STRING, "col3", false),
            new Field(Schema.Type.FLOAT32, "col4", false),
            new Field(Schema.Type.FLOAT64, "col5", false),
            new Field(Schema.Type.BOOLEAN, "col6", false),
            new Field(Schema.Type.INT8, "col7", false),
            new Field(Schema.Type.INT16, "col8", false)
    ));

    assertEquals(1, actual.size());

    String expected = "ALTER TABLE tableA ADD" + System.lineSeparator() +
            "col1 int NULL," + System.lineSeparator() +
            "col2 bigint NULL," + System.lineSeparator() +
            "col3 varchar(256) NULL," + System.lineSeparator() +
            "col4 real NULL," + System.lineSeparator() +
            "col5 float NULL," + System.lineSeparator() +
            "col6 bit NULL," + System.lineSeparator() +
            "col7 tinyint NULL," + System.lineSeparator() +
            "col8 smallint NULL;";
    assertEquals(expected, actual.get(0));
  }
}
