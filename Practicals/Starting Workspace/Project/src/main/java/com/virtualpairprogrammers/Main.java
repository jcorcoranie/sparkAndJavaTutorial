package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        //Util util = new Util();

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialDataRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> strippedRdd = initialDataRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> wordsRdd = strippedRdd.flatMap( value -> Arrays.asList(value.split("\\s")).iterator());

        JavaRDD<String> removeEmptyLinesRdd = wordsRdd.filter( sentence -> sentence.trim().length() > 0);

        JavaRDD<String> filteredWordsRdd = removeEmptyLinesRdd.filter( value -> Util.isNotBoring(value));

        JavaPairRDD<String, Long> pairsRdd = filteredWordsRdd.mapToPair(value -> new Tuple2<>(value, 1L));

        JavaPairRDD<String, Long> sumedPairsRdd = pairsRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switchedPairsRdd = sumedPairsRdd.mapToPair( tuple -> new Tuple2<Long, String> (tuple._2, tuple._1));

        JavaPairRDD<Long, String> sortedPairsRdd = switchedPairsRdd.sortByKey(false);

        List<Tuple2<Long, String>> samplePairsRdd = sortedPairsRdd.take(10);

        samplePairsRdd.forEach( tuple -> System.out.println(tuple._2 + " has " + tuple._1 + " instances."));


        sc.close();
    }
}
