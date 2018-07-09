package com.sparkTutorial.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Uppercase {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/uppercase.text");
        JavaRDD<String> lowerCaseLines = lines.map(String::toUpperCase);

//        lowerCaseLines.saveAsTextFile("out/uppercase.text");
        lowerCaseLines.foreach(line -> System.out.printf("%s\n", line));
    }
}
