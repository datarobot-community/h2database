package org.h2.ri.external.memory;

import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * @author Pavel Ganelin
 */
public class MemoryCursor implements Cursor {
// ------------------------------ FIELDS ------------------------------

    private MemoryTable table;
    private Row row;

// --------------------------- CONSTRUCTORS ---------------------------

    MemoryCursor(MemoryTable  scan) {
        this.table = scan;
        row = null;
    }

// ------------------------ INTERFACE METHODS ------------------------



// --------------------- Interface Cursor ---------------------

    public Row get() {
        return row;
    }

    public SearchRow getSearchRow() {
        return row;
    }

    public boolean next()  {
        row = table.getNextRow(row);
        return row != null;
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
