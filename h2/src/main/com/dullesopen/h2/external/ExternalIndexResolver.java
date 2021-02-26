package com.dullesopen.h2.external;

import org.h2.index.Index;
import org.h2.table.TableLink;

import java.util.List;

public interface ExternalIndexResolver {

    List<Index> getIndexes(TableLink tableLink);

}
