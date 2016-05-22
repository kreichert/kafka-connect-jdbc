/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.datamountaineer.streamreactor.connect.jdbc.sink.Field;
import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides SQL insert support for SQLite
 */
public class SQLiteDialect extends DbDialect {
  public SQLiteDialect() {
    super(getSqlTypeMap());
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "NUMERIC");
    map.put(Schema.Type.INT16, "NUMERIC");
    map.put(Schema.Type.INT32, "NUMERIC");
    map.put(Schema.Type.INT64, "NUMERIC");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "REAL");
    map.put(Schema.Type.BOOLEAN, "NUMERIC");
    map.put(Schema.Type.STRING, "TEXT");
    map.put(Schema.Type.BYTES, "BLOB");
    return map;
  }

  /**
   * Returns the query for creating a new table in the database
   *
   * @param table
   * @param fields
   * @return The create query for the dialect
   */
  public String getCreateQuery(String table, Collection<Field> fields) {
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is not valid.Not accepting empty collection of fields.");
    }
    final StringBuilder builder = new StringBuilder();
    builder.append(String.format("CREATE TABLE %s (", table));
    boolean first = true;
    List<String> primaryKeys = new ArrayList<>();
    for (final Field f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(System.lineSeparator());
      builder.append(f.getName());
      builder.append(" ");
      builder.append(getSqlType(f.getType()));

      if (f.isPrimaryKey()) {
        builder.append(" NOT NULL ");
        primaryKeys.add(f.getName());
      } else {
        builder.append(" NULL");
      }
    }
    if (primaryKeys.size() > 0) {
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("PRIMARY KEY(");
      builder.append(Joiner.on(",").join(primaryKeys));
      builder.append(")");
    }
    builder.append(");");
    return builder.toString();
  }

  @Override
  public List<String> getAlterTable(String table, Collection<Field> fields) {
    ParameterValidator.notNullOrEmpty(table, "table");
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is empty.");
    }
    final List<String> queries = new ArrayList<>(fields.size());
    for (final Field f : fields) {
      queries.add(String.format("ALTER TABLE %s ADD %s %s NULL;", table, f.getName(), getSqlType(f.getType())));
    }
    return queries;
  }

  @Override
  public String getUpsertQuery(String table, List<String> nonKeyColumns, List<String> keyColumns) {
    if (table == null || table.trim().length() == 0)
      throw new IllegalArgumentException("<table> is not a valid parameter");
    if (nonKeyColumns == null || nonKeyColumns.size() == 0)
      throw new IllegalArgumentException("<columns> is invalid.Expecting non null and non empty collection");
    if (keyColumns == null || keyColumns.size() == 0) {
      throw new IllegalArgumentException("<keyColumns> is invalid. Need to be non null, non empty and be a subset of <columns>");
    }
    final String queryColumns = Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns));
    final String bindingValues = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));

    final StringBuilder builder = new StringBuilder();
    builder.append(String.format("%s=?", nonKeyColumns.get(0)));
    for (int i = 1; i < nonKeyColumns.size(); ++i) {
      builder.append(String.format(",%s=?", nonKeyColumns.get(i)));
    }

    final StringBuilder whereBuilder = new StringBuilder();
    whereBuilder.append(String.format("%s=?", keyColumns.get(0)));
    for (int i = 1; i < keyColumns.size(); ++i) {
      whereBuilder.append(String.format(" and %s=?", keyColumns.get(i)));
    }

    return String.format("update or ignore %s set %s where %s\n;", table, builder, whereBuilder) +
            String.format("insert or ignore into %s(%s) values (%s)", table, queryColumns, bindingValues);
  }
}