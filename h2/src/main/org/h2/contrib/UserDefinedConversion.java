package org.h2.contrib;

import org.h2.value.Value;

public interface UserDefinedConversion {
    Value convertTo(Value from, int to);
}
