package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Processor {

    public void process(){


        //Util util = new Util();

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> courseChaptersRdd = sc.textFile("src/main/resources/viewing figures/chapters.csv");

        JavaPairRDD<Integer, Long> courseChapterCountRdd = courseChaptersRdd.mapToPair(rawdata -> new Tuple2<>(Integer.valueOf(rawdata.split(",")[1]), 1L));

        JavaPairRDD<Integer, Long> courseChapterTotalsRdd = courseChapterCountRdd.reduceByKey((value1, value2) -> value1 + value2 );

        //courseChapterTotalsRdd.foreach( tuple -> System.out.println("Course " + tuple._1 + " has " + tuple._2 + " chapters."));


        JavaRDD<String> viewsRdd = sc.textFile("src/main/resources/viewing figures/views-1.csv");

        JavaPairRDD<Integer, Integer> userIdChapterIdRdd = viewsRdd.mapToPair(rawdata -> new Tuple2<>(Integer.valueOf(rawdata.split(",")[0]), Integer.valueOf(rawdata.split(",")[1])));

        JavaPairRDD<String, Long> userIdChapterIdCountRdd = userIdChapterIdRdd.mapToPair( values -> new Tuple2<>( String.valueOf(values._1) + ":" + String.valueOf(values._2) , 1L ));

        JavaPairRDD<String, Long> userIdChapterIdTotalsRdd = userIdChapterIdCountRdd.reduceByKey((value1, value2) -> value1 + value2 );

        userIdChapterIdRdd = userIdChapterIdTotalsRdd.mapToPair(rawdata -> new Tuple2<>( Integer.valueOf(rawdata._1.split(":")[0]), Integer.valueOf(rawdata._1.split(":")[1])));

        userIdChapterIdRdd.foreach( row -> System.out.println(row));




        sc.close();

    }



}
