package com.datinko.prototype.bigdata.spark.core;


import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;

import java.util.List;

/**
 * A simple application that loads a Json file containing data about flights and executes a SQL query against
 * that data using Spark.
 *
 * to run this example:
 *     if you are using IntelliJ - just run it this class! :)
 */
public class SimpleJsonAnalyser {

    private static final Logger LOGGER = Logger.getLogger(SimpleJsonAnalyser.class);

    public static void main(String[] args) {

        String masterName = "local[2]";

        SparkConf conf = new SparkConf().setAppName("Datinko Data Analysis Prototype");

        if (args.length > 0) {
            conf.setMaster(args[0]);
        }
        else {
            conf.setMaster(masterName);
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        // A JSON dataset is pointed to by path.
        // The path can be either a single text file or a directory storing text files.
        //final String peopleDataPath = "src/main/resources/people.json";
        final String flightDataPath = "src/main/resources/flight.json";
        String dataPath = flightDataPath;


        // Create a DataFrame from the file(s) pointed to by path
        DataFrame flights = sqlContext.jsonFile(dataPath);


        // The inferred schema can be visualized using the printSchema() method.
        flights.printSchema();
        // eg: root
        //  |-- age: integer (nullable = true)
        //  |-- name: string (nullable = true)

        // Register this DataFrame as a table.
        flights.registerTempTable("flights");

        // SQL statements can be run by using the sql methods provided by sqlContext.
        //DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
        DataFrame queryResults = sqlContext.sql("SELECT name, Count(*) as Cnt FROM flights GROUP BY name");

        if(queryResults!=null) {
            List<Row> results = queryResults.collectAsList();
            for (Row row : results) {
                LOGGER.warn("The query result is: " + row.get(0).toString() + " " + row.get(1).toString());
                //System.out.printf();
            }
        }
        sc.stop();
    }
}
