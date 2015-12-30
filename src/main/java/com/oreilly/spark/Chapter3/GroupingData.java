package com.oreilly.spark.Chapter3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by brajakumar on 12/30/15.
 */
public class GroupingData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GroupingData");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> shoppingCart1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("bob", "shampoo"),
                new Tuple2<>("alice", "diapers"), new Tuple2<>("diaz", "beer"), new Tuple2<>("diaz", "cake"),new
                        Tuple2<>("steve", "shirt")));
        JavaPairRDD<String, String> shoppingCart2 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("bob", "cereals"),
                new Tuple2<>("alice", ""), new Tuple2<>("steve", "pants"), new Tuple2<>("steve", "tie")));

        //List down all the items for each customer
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> result = shoppingCart1.cogroup(shoppingCart2);

        System.out.println("Shopping cart for each customer : ");
        System.out.println(result.collect());

        //Find ratings of a store based on location
        JavaPairRDD<String, String> storeAddress = sc.parallelizePairs(Arrays.asList(new Tuple2<>("Macy", "1026 " +
                        "Valencia St"), new Tuple2<>("Philz", "748 Van Nes Ave"), new Tuple2<>("Philz", "3101 24th St"),
                        new Tuple2<>("Starbucks", "Seattle")));

        JavaPairRDD<String, Double> storeRatings = sc.parallelizePairs(Arrays.asList(new Tuple2<>("Macy", 4.3), new
                Tuple2<>("Philz", 4.5)));

        JavaPairRDD<String, Tuple2<String, Double>> ratings = storeAddress.join(storeRatings);
        System.out.println("Store ratings : ");
        System.out.println(ratings.collect());

        //Left Outer Join
        System.out.println("List all store with/without ratings : ");
        System.out.println(storeAddress.leftOuterJoin(storeRatings).collect());

        //Right Outer Join
        System.out.println("List all the rating with/without stores : ");
        System.out.println(storeAddress.rightOuterJoin(storeRatings).collect());

        //Sorting Stores based on name
        System.out.println("Store name sorted in descending order");
        JavaPairRDD<String, String> sortedStore = storeAddress.sortByKey(new StoreNameComparator());
        System.out.println(sortedStore.collect());
    }
}

class StoreNameComparator implements Comparator<String>, Serializable {

    @Override
    public int compare(String s1, String s2) {
        return String.valueOf(s2).compareTo(String.valueOf(s1));
    }
}
