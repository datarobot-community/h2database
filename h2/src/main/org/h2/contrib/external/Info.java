package org.h2.contrib.external;

import org.h2.command.ddl.CreateTableData;
import org.h2.index.IndexType;
import org.h2.table.Column;
import org.h2.table.IndexColumn;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Serialized meta information about database table.
 *
 * @author Pavel Ganelin
 */
public class Info {
// ------------------------------ FIELDS ------------------------------

    /**
     * Key to look up table info
     */
    public static final String TABLE = "#self#";

    /**
     * name of the map with table meta information
     */
    public static final String TABLES = "tables";
    /**
     * name of the map with primary index
     */
    public static final String PRIMARY = "primary";
    /**
     * name of the map with meta information about secondary indices
     */
    static final String INDICES = "indices";

// -------------------------- INNER CLASSES --------------------------

    public static class Table implements Serializable {
        final java.util.List<ColumnData> columns;

        public Table(CreateTableData data) {
            this.columns = new ArrayList<ColumnData>();
            for (Column column : data.columns) {
                this.columns.add(new ColumnData(column));
            }
        }

        public CreateTableData export() {
            CreateTableData result = new CreateTableData();
            result.columns = new ArrayList<Column>();
            for (ColumnData column : columns) {
                result.columns.add(column.export());
            }
            return result;
        }
    }

    static class ColumnData implements Serializable {
        final int type;
        final int scale;
        final long precision;
        final int displaySize;
        final String name;
        final String originalSql;
        final boolean nullable;
        final boolean primaryKey;

        public ColumnData(Column column) {
            type = column.getType();
            scale = column.getScale();
            precision = column.getPrecision();
            displaySize = column.getDisplaySize();
            name = column.getName();
            originalSql = column.getOriginalSQL();
            nullable = column.isNullable();
            primaryKey = column.isPrimaryKey();
        }

        public Column export() {
            Column column = new Column(name, type, precision, scale, displaySize);
            column.setOriginalSQL(originalSql);
            column.setNullable(nullable);
            column.setPrimaryKey(primaryKey);
            return column;
        }
    }

    static class IndexData implements Serializable {
        final boolean unique;
        final boolean primary;
        final java.util.List<String> columns;

        public IndexData(IndexType indexType, IndexColumn[] columns) {
            unique = indexType.isUnique();
            primary = indexType.isPrimaryKey();
            this.columns = new ArrayList<String>();
            for (IndexColumn column : columns) {
                this.columns.add(column.columnName);
            }
        }

        public IndexType createIndexType() {
            return primary ? IndexType.createPrimaryKey(true, false) :
                    unique ? IndexType.createUnique(true, false) :
                            IndexType.createNonUnique(true);
        }

        public IndexColumn[] export() {
            IndexColumn[] columns = new IndexColumn[this.columns.size()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new IndexColumn();
                columns[i].columnName = this.columns.get(i);
            }
            return columns;
        }
    }
}
