package com.oreilly.spark.Chapter3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by brajakumar on 12/29/15.
 */
public class PairBasics {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairBasics");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Creating a pair using the first word from each line of the file as the key and line as the value
        JavaRDD<String> lines = sc.textFile("src/main/resources/hello.txt");

        JavaPairRDD<String, String> myPair = lines.mapToPair(new FilePair());
        System.out.println("Pair created from file : ");
        System.out.println(myPair.collect());


        JavaPairRDD<Integer, Integer> examplePairs = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1,2), new
                Tuple2<>(3,4), new Tuple2<>(3,6)));

        //Transformations on Pairs
        JavaPairRDD<Integer, Integer> pairsReducedByKeys = examplePairs.reduceByKey(new ReducedByKeys());
        System.out.println("Summed up pairs are : " + pairsReducedByKeys.collect());

        System.out.println("Grouped by keys : " + examplePairs.groupByKey().collect());

        JavaPairRDD<Integer, Integer> sumValuesWithSameKeys = examplePairs.mapValues(new AddXToEachValue());
        System.out.println("Adding 5 to each value of a Pair : " + sumValuesWithSameKeys.collect());

        System.out.println("Keys from each Pair : " + examplePairs.keys().collect());

        System.out.println("Values from each Pair : " + examplePairs.values().collect());

        System.out.println("Sorted by Keys : " + examplePairs.sortByKey(false).collect());

        //Filter
        JavaPairRDD<String, String> stringPairs = sc.parallelizePairs(Arrays.asList(new Tuple2<>
                ("holden", "likes coffee"), new Tuple2<>("panda", "likes long strings and coffee")));

        JavaPairRDD<String, String> filteredPair = stringPairs.filter(new LengthLesserThanX());

        System.out.println("Filtered pair : " + filteredPair.collect());

        //Faster word count
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        System.out.println("Word Count : ");
        Map<String, Long> wordCount = words.countByValue();
        for(Map.Entry<String, Long> e : wordCount.entrySet()) {
            System.out.println(e.getKey() + ", " + e.getValue());
        }

        //Finding average per key
        JavaPairRDD<Integer, Integer> numbers = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1, 2), new Tuple2<>(1,
                4), new Tuple2<>(3, 2), new Tuple2<>(3, 9), new Tuple2<>(4, 2), new Tuple2<>(4, 3)));
//        AvgCount initial = new AvgCount(0,0);
        JavaPairRDD<Integer, AvgCount> avgCountPerKey = numbers.combineByKey(new CreateAvgCount(), new AddAndCount(),
                new CombineCount());

        System.out.println("Average Per Key are : ");
        Map<Integer, AvgCount> avgCountPerKeyMap = avgCountPerKey.collectAsMap();
        for(Map.Entry<Integer, AvgCount> e: avgCountPerKeyMap.entrySet()) {
            System.out.println(e.getKey() + ", " + e.getValue().avg());
        }
    }

    public static class FilePair implements PairFunction<String, String, String> {

        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            String key = s.split(" ")[0];
            return new Tuple2<>(key, s);
        }
    }

    public static class ReducedByKeys implements Function2<Integer, Integer, Integer> {

        @Override
        public Integer call(Integer x, Integer y) throws Exception {
            return x + y;
        }
    }

    public static class AddXToEachValue implements Function<Integer, Integer> {

        @Override
        public Integer call(Integer x) throws Exception {
            return x + 5;
        }
    }

    public static class LengthLesserThanX implements Function<Tuple2<String, String>, Boolean> {

        @Override
        public Boolean call(Tuple2<String, String> tuple) throws Exception {
            if(tuple._2().length() > 15)
                return true;
            else
                return false;
        }
    }

    public static class AvgCount implements Serializable {
        public int total;
        public int count;

        public AvgCount(int total, int count) {
            this.total = total;
            this.count = count;
        }

        public float avg() {
            return total/(float) count;
        }
    }

    //Initialize each element
    public static class CreateAvgCount implements Function<Integer, AvgCount> {

        @Override
        public AvgCount call(Integer x) throws Exception {
            return new AvgCount(x,1);
        }
    }

    //Find average for the same keys in the same partition
    public static class AddAndCount implements Function2<AvgCount, Integer, AvgCount> {

        @Override
        public AvgCount call(AvgCount a, Integer x) throws Exception {
            a.total = a.total + x;
            a.count = a.count + 1;
            return a;
        }
    }

    //Find final average for the same keys across different partition to produce the final average for the given key
    public static class CombineCount implements Function2<AvgCount, AvgCount, AvgCount> {

        @Override
        public AvgCount call(AvgCount a, AvgCount b) throws Exception {
            a.total = a.total + b.total;
            a.count = a.count + b.count;
            return a;
        }
    }

}
