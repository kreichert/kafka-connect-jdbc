package io.confluent.connect.jdbc.source;


import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class DataConverterTest {

    @Test
    public void convertsJSONToString() throws SQLException {
        final String tableName = "test";
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        rowSetMetaData.setColumnType(1, Types.OTHER);
        rowSetMetaData.setColumnTypeName(1, "JSON");
        final ResultSetMetaData metaData = rowSetMetaData;
        Schema schema = DataConverter.convertSchema(tableName, metaData);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected JSON to be converted to a string", Schema.Type.STRING, schema.fields().get(0).schema().type());
    }

    @Test
    public void convertsJSONBToString() throws SQLException {
        final String tableName = "test";
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        rowSetMetaData.setColumnType(1, Types.OTHER);
        rowSetMetaData.setColumnTypeName(1, "JSONB");
        final ResultSetMetaData metaData = rowSetMetaData;
        Schema schema = DataConverter.convertSchema(tableName, metaData);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected JSONB to be converted to a string", Schema.Type.STRING, schema.fields().get(0).schema().type());
    }

    @Test
    public void convertsUUIDToString() throws SQLException {
        final String tableName = "test";
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        rowSetMetaData.setColumnType(1, Types.OTHER);
        rowSetMetaData.setColumnTypeName(1, "UUID");
        final ResultSetMetaData metaData = rowSetMetaData;
        Schema schema = DataConverter.convertSchema(tableName, metaData);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected UUID to be converted to a string", Schema.Type.STRING, schema.fields().get(0).schema().type());
    }

    @Test
    public void supportsArrayType() throws SQLException {
        final String tableName = "test";
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        rowSetMetaData.setColumnType(1, Types.ARRAY);
        rowSetMetaData.setColumnTypeName(1, "ARRAY");
        final ResultSetMetaData metaData = rowSetMetaData;
        Schema schema = DataConverter.convertSchema(tableName, metaData);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected Array to be supported", Schema.Type.ARRAY, schema.fields().get(0).schema().type());
    }
}
