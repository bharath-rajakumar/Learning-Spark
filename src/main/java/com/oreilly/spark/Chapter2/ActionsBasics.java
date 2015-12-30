package com.oreilly.spark.Chapter2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by brajakumar on 12/28/15.
 */
public class ActionsBasics {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ActionsBasics");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> listOfNumbers = sc.parallelize(Arrays.asList(1,2,3,4));

        Integer sum = listOfNumbers.reduce(new SumUp());

        System.out.println("Final Sum : " + sum);

        AvgCount initial = new AvgCount(0, 0);
        AvgCount runningAverage =  listOfNumbers.aggregate(initial, new addAndCount(), new combine());
        System.out.println("Average is : " + runningAverage.avg());

        //Explicit RDD types
        JavaDoubleRDD listOfDoubles = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
        JavaDoubleRDD squares = listOfDoubles.mapToDouble(new SquareUp());

        System.out.println("Squared values of doubles list are : " + squares.collect());
        System.out.println("Mean is : " + squares.mean());
        System.out.println("Variance is : " + squares.variance());

        //Persistence (Caching)
        System.out.println("Persisting squares RDD to avoid re-computation");
        squares.persist(StorageLevel.DISK_ONLY());
        System.out.println("Mean is : " + squares.mean());
        System.out.println("Variance is : " + squares.variance());
    }

    public static class SumUp implements Function2<Integer, Integer, Integer> {

        @Override
        public Integer call(Integer x, Integer y) throws Exception {
            return x + y;
        }
    }

    public static class SquareUp implements DoubleFunction<Double> {

        @Override
        public double call(Double x) throws Exception {
            System.out.println("I got called !!!");
            return x * x;
        }
    }

    static class AvgCount implements Serializable {
        int total;
        int num;
        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public double avg() {
            return total/(double)num;
        }
    }

    public static class addAndCount implements Function2<AvgCount, Integer, AvgCount> {

        @Override
        public AvgCount call(AvgCount a, Integer x) throws Exception {
            a.total = a.total + x;
            a.num = a.num + 1;
            System.out.println("Total : " + a.total);
            System.out.println("Num : " + a.num);
            return a;
        }
    }

    public static class combine implements Function2<AvgCount, AvgCount, AvgCount> {

        @Override
        public AvgCount call(AvgCount a, AvgCount b) throws Exception {
            a.total = a.total + b.total;
            a.num = a.num + b.num;
            System.out.println("B's total : " + b.total);
            System.out.println("B's count : " + b.num);
            System.out.println("Running total : " + a.total);
            System.out.println("Running count : " + a.num);
            return a;
        }
    }
}




