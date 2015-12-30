package com.oreilly.spark.Chapter2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by brajakumar on 12/28/15.
 */
public class PseudoSetOperations {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PseudoSetOperations");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Sample data-set
        JavaRDD<String> firstRDD = sc.parallelize(Arrays.asList("coffee", "coffee", "panda", "monkey", "tea"));

        JavaRDD<String> secondRDD = sc.parallelize(Arrays.asList("coffee", "monkey", "kitty"));

        //find unique elements
        JavaRDD<String> distinctRDD = firstRDD.distinct();
        System.out.println("Distinct elements : " + distinctRDD.collect());

        //find union of both sets
        JavaRDD<String> unionizedRDD = firstRDD.union(secondRDD);
        System.out.println("Union of two sets are : " + unionizedRDD.collect());

        //find intersection of both sets
        JavaRDD<String> intersectionizedRDD = firstRDD.intersection(secondRDD);
        System.out.println("Intersection of two sets are : " + intersectionizedRDD.collect());

        //find subtraction of both sets
        JavaRDD<String> subtractionizedRDD = firstRDD.subtract(secondRDD);
        System.out.println("Subtraction of two sets are : " + subtractionizedRDD.collect());
    }
}
