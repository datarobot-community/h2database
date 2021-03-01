package org.h2.contrib.external;

import org.h2.index.Index;
import org.h2.table.TableLink;

import java.util.List;

public interface ExternalIndexResolver {

    List<Index> getIndexes(TableLink tableLink);

}
