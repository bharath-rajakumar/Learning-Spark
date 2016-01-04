package com.oreilly.spark.Chapter5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by brajakumar on 1/4/16.
 */
public class BroadCasts {
    public static void main(String[] args) {
        //Create a java spark context
        SparkConf conf = new SparkConf().setAppName("BroadCasts");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("src/main/resources/callSigns.txt");
        JavaRDD<String> callSigns = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(", "));
            }
        });

        callSigns.saveAsTextFile("callSigns");

        //Filter out valid call signs
        JavaRDD<String> validCallSigns = callSigns.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String callSign) throws Exception {
                Pattern p = Pattern.compile("\\A\\d?\\p{Alpha}{1,2}\\d{1,4}\\p{Alpha}{1,3}\\Z");
                Matcher m = p.matcher(callSign);
                if(m.matches()) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        System.out.println("number of valid call signs are : " + validCallSigns.count());

        //find number of contacts made for each call sign
        JavaPairRDD<String, Integer> contactCounts = validCallSigns.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String callSign) throws Exception {
                return new Tuple2<>(callSign, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });

        System.out.println("Contact Counts for each call sign : ");
        System.out.println(contactCounts.collect());

        final Broadcast<HashMap<String, String>> signPrefixes = sc.broadcast(loadCallSignTable());
//        System.out.println("Sign Prefixes : ");
//        System.out.println(signPrefixes.value().toString());
        JavaPairRDD<String, Integer> countryContactCounts = contactCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> callSignCount) throws Exception {
                String sign = callSignCount._1();
                String country = lookupCountry(sign, signPrefixes.value());
                return new Tuple2<>(country, callSignCount._2());
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });

        countryContactCounts.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("Call signs contact counts by country : ");
        System.out.println(countryContactCounts.collect());
    }

    private static String lookupCountry(String sign, HashMap<String, String> value) {
        return value.get(sign);
    }

    private static HashMap<String, String> loadCallSignTable() {
        HashMap<String, String> callSigns = new HashMap<>();
        try {
            Scanner in  = new Scanner(new File("src/main/resources/callSigns.txt"));
            while(in.hasNext()) {
                String[] line = in.nextLine().split(", ");
                if(line.length == 2) {
                    String key = line[0];
                    String value = line[1];
                    callSigns.put(key, value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return callSigns;
    }
}
