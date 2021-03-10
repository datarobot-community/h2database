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
