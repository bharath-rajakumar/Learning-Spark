package com.oreilly.spark.Chapter1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by brajakumar on 12/28/15.
 */
public class wordCount {
    public static void main(String[] args) {
        //Create a java spark context
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Load input data
        JavaRDD<String> input = sc.textFile("src/main/resources/hello.txt");

        //Split each lines into words
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                }
        );

        //Transform into word and counts
        JavaPairRDD<String,Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer x, Integer y) throws Exception {
                        return x + y;
                    }
                }
        );

        //Save output to text files
        counts.saveAsTextFile("outputFile");
    }
}
