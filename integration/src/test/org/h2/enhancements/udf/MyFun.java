package org.h2.enhancements.udf;

import org.h2.expression.Expression;

import java.lang.reflect.Method;

public class MyFun {

    @SuppressWarnings("unused")
    public static long myprecision(Method java, Expression[] args) {
        return 10 * args[0].getPrecision() + 3;
    }

    public static String fs(String s) {
        return s;
    }


}
