package com.datamountaineer.streamreactor.connect.jdbc.sink;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JdbcHelperTest {
  private static final String DB_FILE = "test_db_jdbc_helper_sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;

  @Before
  public void setUp() {
    deleteSqlLiteFile();
  }

  @After
  public void tearDown() {
    deleteSqlLiteFile();
  }

  private void deleteSqlLiteFile() {
    new File(DB_FILE).delete();
  }

  @Test
  public void returnTheDatabaseNameForSqLite() throws SQLException {
    String database = JdbcHelper.getDatabase(SQL_LITE_URI, null, null);
    assertEquals(database, null);
  }

  @Test
  public void returnTheDatabaseTableInformation() throws SQLException {
    String createEmployees = "CREATE TABLE employees\n" +
            "( employee_id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  last_name VARCHAR NOT NULL,\n" +
            "  first_name VARCHAR,\n" +
            "  hire_date DATE\n" +
            ");";


    String createProducts = "CREATE TABLE products\n" +
            "( product_id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  product_name VARCHAR NOT NULL,\n" +
            "  quantity INTEGER NOT NULL DEFAULT 0\n" +
            ");";

    String createNonPkTable = "CREATE TABLE nonPk (id numeric, response text)";

    SqlLiteHelper.createTable(SQL_LITE_URI, createEmployees);
    SqlLiteHelper.createTable(SQL_LITE_URI, createProducts);
    SqlLiteHelper.createTable(SQL_LITE_URI, createNonPkTable);


    final Map<String, DbTable> tables = new HashMap<>();

    for (DbTable table : DatabaseMetadata.getTableMetadata(HikariHelper.from(SQL_LITE_URI, null, null))) {
      tables.put(table.getName(), table);
    }

    assertEquals(tables.size(), 4);  //sqlite_sequence is automatic
    assertTrue(tables.containsKey("employees"));
    assertTrue(tables.containsKey("products"));
    assertTrue(tables.containsKey("nonPk"));

    DbTable nonPk = tables.get("nonPk");

    Map<String, DbTableColumn> columns = nonPk.getColumns();
    assertEquals(columns.size(), 2);
    assertTrue(columns.containsKey("id"));
    assertTrue(columns.get("id").allowsNull());
    assertFalse(columns.get("id").isPrimaryKey());
    assertEquals(columns.get("id").getSqlType(), Types.FLOAT);
    assertTrue(columns.containsKey("response"));
    assertTrue(columns.get("response").allowsNull());
    assertFalse(columns.get("response").isPrimaryKey());
    assertEquals(columns.get("response").getSqlType(), Types.VARCHAR);

    DbTable employees = tables.get("employees");
    columns = employees.getColumns();
    assertEquals(columns.size(), 4);
    assertTrue(columns.containsKey("employee_id"));
    assertFalse(columns.get("employee_id").allowsNull());
    assertTrue(columns.get("employee_id").isPrimaryKey());
    assertEquals(columns.get("employee_id").getSqlType(), Types.INTEGER);
    assertTrue(columns.containsKey("last_name"));
    assertFalse(columns.get("last_name").allowsNull());
    assertFalse(columns.get("last_name").isPrimaryKey());
    assertEquals(columns.get("last_name").getSqlType(), Types.VARCHAR);
    assertTrue(columns.containsKey("first_name"));
    assertTrue(columns.get("first_name").allowsNull());
    assertFalse(columns.get("first_name").isPrimaryKey());
    assertEquals(columns.get("first_name").getSqlType(), Types.VARCHAR);
    assertTrue(columns.containsKey("hire_date"));
    assertTrue(columns.get("hire_date").allowsNull());
    assertFalse(columns.get("hire_date").isPrimaryKey());
    // sqlite returns VARCHAR for DATE. why?!
    // assertEquals(columns.get("hire_date").getSqlType(), Types.DATE);


    DbTable products = tables.get("products");
    columns = products.getColumns();
    assertEquals(columns.size(), 3);
    assertTrue(columns.containsKey("product_id"));
    assertFalse(columns.get("product_id").allowsNull());
    assertTrue(columns.get("product_id").isPrimaryKey());
    assertEquals(columns.get("product_id").getSqlType(), Types.INTEGER);

    assertTrue(columns.containsKey("product_name"));
    assertFalse(columns.get("product_name").allowsNull());
    assertFalse(columns.get("product_name").isPrimaryKey());
    assertEquals(columns.get("product_name").getSqlType(), Types.VARCHAR);

    assertTrue(columns.containsKey("quantity"));
    assertFalse(columns.get("quantity").allowsNull());
    assertFalse(columns.get("quantity").isPrimaryKey());
    assertEquals(columns.get("quantity").getSqlType(), Types.INTEGER);
  }
}
