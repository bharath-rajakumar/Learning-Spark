package com.oreilly.spark.Chapter6;

import org.json.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by brajakumar on 1/5/16.
 */
public class TweetCollector {
    public static String CONSUMER_KEY = System.getenv("CONSUMER_KEY");
    public static String CONSUMER_SECRET = System.getenv("CONSUMER_SECRET");
    public static String ACCESS_TOKEN = System.getenv("ACCESS_TOKEN");
    public static String ACCESS_TOKEN_SECRET = System.getenv("ACCESS_TOKEN_SECRET");

    public static void main(String[] args) {
        //Enable JSON format response
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setJSONStoreEnabled(true);

        //Configure the API keys
        Twitter twitter = new TwitterFactory(configurationBuilder.build()).getInstance();
        twitter.setOAuthConsumer(CONSUMER_KEY, CONSUMER_SECRET);
        AccessToken accessToken = new AccessToken(ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
        twitter.setOAuthAccessToken(accessToken);

        //Search for tweets
        String queryString = "ps4";

        File file = new File("tweets/" + queryString + ".json");
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            Query query = new Query(queryString);
            int count = 0;
            try {
                twitter4j.QueryResult queryResult = twitter.search(query);
                JSONArray jsonArray = new JSONArray();
                for(Status status : queryResult.getTweets()) {
                    //Convert response to JSON
                    String statusJSON = TwitterObjectFactory.getRawJSON(status);
                    JSONObject tweet = new JSONObject();
                    try {
                        tweet.put("username", status.getUser().getScreenName());
                        tweet.put("retweetCount", status.getRetweetCount());
                        tweet.put("location", status.getUser().getTimeZone());
                        tweet.put("body", status.getText());
                        jsonArray.put(count,tweet);
                        count = count + 1;
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                    bw.write(jsonArray.toString());
//                    System.out.println("JSON Response : " + statusJSON);
                    System.out.println("@" + status.getUser().getScreenName() + " : " + status.getText());
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
