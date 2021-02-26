/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

/**
 * This class contains information about a built-in function.
 */
public class FunctionInfo {

    /**
     * The name of the function.
     */
    String name;

    /**
     * The function type.
     */
    int type;

    /**
     * The data type of the return value.
     */
    int returnDataType;

    /**
     * The number of parameters.
     */
    int parameterCount;

    /**
     * If the result of the function is NULL if any of the parameters is NULL.
     */
    boolean nullIfParameterIsNull;

    /**
     * If this function always returns the same value for the same parameters.
     */
    boolean deterministic;

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     */
    boolean bufferResultSetToLocalTemp = true;

    /* defines how to calculate precision of the result using the arguments
       of the argument.

       This information can also be passes when define user defined function
    */
    public static final int
            LEGACY = 0,    // old behaviour where the result is zero
            ARG = 1,      // the precision of the result is the same as precision of the argument,
                          // argument number is specified by precision
            SUM = 2,      // the precision of the result equal sum of the argument
            FIXED = 3;    // the precision of the result is set up to some fixed number

    int precisionMethod;
    long precision;

    static long calculatePrecision(int precisionMethod, long precision, Expression[] args) {

        if (args.length==0)
            return 0;
        long p = args[0].getPrecision();
        switch (precisionMethod) {
            case LEGACY :
                return 0;
            case ARG :
                // TODO validate precision against array size
                return args[((int) precision)].getPrecision();
            case SUM :
                int sum = 0;
                for (int i = 0; i < args.length; i++) {
                    Expression arg = args[i];
                    sum += arg.getPrecision();
                }
                return sum;
            case FIXED :
                return precision;
            default:
                throw new IllegalArgumentException(); // TODO for TM, throw correct exception here
        }
    }
}
