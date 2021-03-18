package org.h2.contrib;

import org.h2.value.Value;

/**
 * Used to convert arguments of user defined function to circumvent limitations of function overloading in H2
 */
public interface UdfArgumentConverter {
    Value convertTo(Value value, int fromType, int toType);
}
