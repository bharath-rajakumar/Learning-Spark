package com.oreilly.spark.Chapter6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 * Created by brajakumar on 1/5/16.
 */
public class BasicSparkSQL {
    public static void main(String[] args) {
        //Create a java spark context
        SparkConf conf = new SparkConf().setAppName("BasicSparkSQL");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame input = sqlContext.jsonFile("src/main/resources/tweets/ps4.json");
        //Register the input schema RDD
        input.registerTempTable("tweets");
        //select tweets based on re-tweets count
        DataFrame topReTweetedTweets = sqlContext.sql("SELECT text, retweet_count FROM tweets ORDER BY " +
                "retweet_count");
        System.out.println("Query Result :");
        int count  = 0;

        //Cache the dataframe before printing
        sqlContext.cacheTable("tweets");
        for(Row row : topReTweetedTweets.collect()) {
            if(row.get(0).toString() != null && row.get(1).toString() != null && row.length() == 2) {
                System.out.println(count + ", "+ row.get(0).toString() + ", " + row.get(1).toString());
            }
            count = count + 1;
        }
    }
}
