/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.contrib.link;

import org.h2.index.Index;
import org.h2.table.TableLink;

import java.util.List;

public interface LinkedIndexResolver {

    /**
     * Used to add additional indexes for Hadoop linked tables
     */
    List<Index> getIndexes(TableLink tableLink);

}
