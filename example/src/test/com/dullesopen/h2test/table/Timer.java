package com.dullesopen.h2test.table;

class Timer {
    long start;

    Timer() {
        this.start = System.currentTimeMillis();
    }

    void report(String message) {
        long end = System.currentTimeMillis();
        double elapsed = (end - start) / 1000.0;
        System.out.println(message + " " + elapsed + " sec.");
    }
}
