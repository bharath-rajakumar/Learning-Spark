package com.oreilly.spark.Chapter5;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * Created by brajakumar on 1/4/16.
 */
public class Accumulators {
    public static void main(String[] args) {
        //Create a java spark context
        SparkConf conf = new SparkConf().setAppName("Accumulators");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Create an accumulator to keep track of number of blank lines in callSigns.txt
        final Accumulator<Integer> blankLines = sc.accumulator(0);

        JavaRDD<String> input = sc.textFile("src/main/resources/callSigns.txt");

        JavaRDD<String> callSigns = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                if(s.equals("")) {
                    blankLines.add(1);
                }
                return Arrays.asList(s.split(" "));
            }
        });

        callSigns.saveAsTextFile("Chapter5-Output");
        System.out.println("Number of blank lines present in text file : " + blankLines);
    }
}
