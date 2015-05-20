package com.datinko.prototype.bigdata.spark.core;

import com.datinko.prototype.bigdata.spark.core.helpers.TwitterHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.util.HashMap;


/**
 * Created by neildunlop on 19/05/2015.
 */
public class TwitterStreamAnalyser {

    public static void main(String[] args) throws Exception {

        String masterName = "local[2]";

        SparkConf conf = new SparkConf().setAppName("Datinko Data Analysis Prototype");

        if (args.length > 0) {
            conf.setMaster(args[0]);
        }
        else {
            conf.setMaster(masterName);
        }

        //set checkpoint directory
        String checkpointDirectory = TwitterHelper.getCheckpointDirectory();

        //Configure Twitter credentials
        String apiKey = "WEFvQcyvRSFsCyEZQrz0AwG29";
        String apiSecret = "zQ1NrVXJbHqotXtzG78RR2Lgkg7CziVW5ZNQSc03UuOQ8fZPyQ";
        String accessToken = "21389069-ZLfprdl8q8ToQnBCfVnEp25aPlS1apIywKQgnFS4d";
        String accessTokenSecret = "JZNqfkdYN2Ng6FmE7eN0XfPLnx8SUbE3XWpv93suBukAQ";

        configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret);

        //setup streaming context
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));

        //create a stream of tweets - continious stream of twitter4j.Status objects
        JavaDStream<Status> tweets = TwitterUtils.createStream(streamingContext);

        //transform the stream of status objects into a stream of status text - this the guts of our operation
        JavaDStream<String> statuses = tweets.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return status.getText();
                    }
                }
        );
        statuses.print();

        //set the checkpoint directory for checkpointing of intermediate data
        streamingContext.checkpoint(checkpointDirectory);

        //start the context and keep going until interrupted
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    static void configureTwitterCredentials(String apiKey, String apiSecret, String accessToken, String accessTokenSecret) throws Exception {
        HashMap<String, String> configs = new HashMap<String, String>();
        configs.put("apiKey", apiKey);
        configs.put("apiSecret", apiSecret);
        configs.put("accessToken", accessToken);
        configs.put("accessTokenSecret", accessTokenSecret);

        Object[] keys = configs.keySet().toArray();
        for (int k = 0; k < keys.length; k++) {
            String key = keys[k].toString();
            String value = configs.get(key).trim();
            if (value.isEmpty()) {
                throw new Exception("Error setting authentication - value for " + key + " not set");
            }
            String fullKey = "twitter4j.oauth." + key.replace("api", "consumer");
            System.setProperty(fullKey, value);
            System.out.println("\tProperty " + key + " set as [" + value + "]");
        }
        System.out.println();
    }
}
