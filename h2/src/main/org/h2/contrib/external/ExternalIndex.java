/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.contrib.external;

import org.h2.index.BaseIndex;

public abstract class ExternalIndex extends BaseIndex {

    public abstract long getRowCountMax();

    public abstract int getMainIndexColumn();

    public abstract void setMainIndexColumn(int mainIndexColumn);
}
