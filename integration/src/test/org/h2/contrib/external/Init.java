package org.h2.contrib.external;

import org.h2.ri.external.disk.DiskSchemaFactory;
import org.h2.ri.external.memory.MemorySchemaFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

public class Init {
// ------------------------------ FIELDS ------------------------------

    /**
     * switch back and forth between build in tables and external schema for testing
     */
    private static boolean DISK = true;

    private static boolean MEMORY = false;

// -------------------------- STATIC METHODS --------------------------

    static String schema(String filename) {
        String disk = MessageFormat.format("CREATE SCHEMA S EXTERNAL (''{0}'',''dir={1}'')",
                DiskSchemaFactory.class.getName(), filename);
        String memory = MessageFormat.format("CREATE SCHEMA S EXTERNAL (''{0}'',''{1}'')",
                MemorySchemaFactory.class.getName(), filename);
        return DISK ?
                disk :
                MEMORY ?
                        memory :
                        "CREATE SCHEMA S ";
    }
}
