package com.oreilly.spark.Chapter2;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * Created by brajakumar on 12/28/15.
 */
public class RDDBasics {
    public static void main(String[] args) {
        //Create a java spark context
        SparkConf conf = new SparkConf().setAppName("RDDBasics");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("src/main/resources/sampleLog.txt");

        //Filter lines that contains only the word - Machintosh
        JavaRDD<String> mac = input.filter(new ContainsWord("Macintosh"));

        //Filter lines that contains only the word - Windows
        JavaRDD<String> windows = input.filter(new ContainsWord("Windows"));

        //Print the results
        System.out.println("Windows RDD : ");
        for(String line : windows.take(10)) {
            System.out.println(line);
        }

        System.out.println("Mac RDD : ");
        for(String line : mac.take(10)) {
            System.out.println(line);
        }

        //Create a new list with numbers
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));

        JavaRDD<Integer> squaredNumbers = numbers.map(new SquareIt());

        System.out.println("Squared Numbers are : " + StringUtils.join(squaredNumbers.collect(),","));

        //Load input data
        JavaRDD<String> inputTwo = sc.textFile("src/main/resources/hello.txt");

        //Split the entire file into words
        JavaRDD<String> words = inputTwo.flatMap(new LineTokenizer());

        System.out.println("Tokenized Output");
        System.out.println(words.collect());

        //Split each line into words
        JavaRDD<String> linesOfWords = inputTwo.map(new WordTokenizer());

        System.out.println("Line Tokenized Output");
        System.out.println(linesOfWords.collect());

    }

    public static class WordTokenizer implements Function<String, String> {
        @Override
        public String call(String s) throws Exception {
            return String.valueOf(Arrays.asList(s.split(" ")));
        }
    }

    public static class LineTokenizer implements FlatMapFunction<String, String> {
        @Override
        public Iterable<String> call(String s) throws Exception {
            return Arrays.asList(s.split(" "));
        }
    }

    public static class ContainsWord implements Function<String, Boolean> {
        String theWord;

        public ContainsWord(String theWord) {
            this.theWord = theWord;
        }

        @Override
        public Boolean call(String s) throws Exception {
            return s.contains(theWord);
        }
    }

    public static class SquareIt implements Function<Integer, Integer> {
        @Override
        public Integer call(Integer x) throws Exception {
            return x * x;
        }
    }
}



