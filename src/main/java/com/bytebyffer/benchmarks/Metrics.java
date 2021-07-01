package com.bytebyffer.benchmarks;

public class Metrics {

    private double real;
    private double cpu;
    private double gcTime;
    private long gcCount;

    public double getThroughput() {
        return throughput;
    }

    private double throughput;

    public double getReal() {
        return real;
    }

    public double getCpu() {
        return cpu;
    }

    public double getGcTime() {
        return gcTime;
    }

    public long getGcCount() {
        return gcCount;
    }


    public Metrics(double real, double cpu, double gcTime, long gcCount, double throughput) {
        this.real = real;
        this.cpu = cpu;
        this.gcTime = gcTime;
        this.gcCount = gcCount;
        this.throughput = throughput;
    }
}
