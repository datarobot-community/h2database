package com.dullesopen.h2.external;

import org.h2.index.BaseIndex;

/**
 * @author Pavel Ganelin
 */
public abstract class ExternalIndex extends BaseIndex {
// -------------------------- OTHER METHODS --------------------------

    public abstract long getRowCountMax();

    public abstract int getMainIndexColumn();

    public abstract void setMainIndexColumn(int mainIndexColumn);
}
