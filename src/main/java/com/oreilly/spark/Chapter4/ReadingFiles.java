package com.oreilly.spark.Chapter4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by brajakumar on 12/31/15.
 */
public class ReadingFiles {
    public static void main(String[] args) {
        //Create a java spark context
        SparkConf conf = new SparkConf().setAppName("ReadingFiles");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Creating RDD from multiple files
        JavaPairRDD<String, String> filesContentRDD = sc.wholeTextFiles("src/main/resources/salesNumbers/");

        JavaPairRDD<String, Double> avgSales = filesContentRDD.mapValues(new Function<String, Double>() {
            @Override
            public Double call(String s) throws Exception {
                String[] numbers = s.split("\\n");
                Double sum = 0.0;
                Double avg = 0.0;
                int count = 0;
                for(String n : numbers) {
                    sum = sum + Double.parseDouble(n);
                    count = count + 1;
                }
                avg = sum/count;
                return avg;
            }
        });

        System.out.println("Average sales per File : " + avgSales.collect());
    }
}
