package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {

        //Util util = new Util();

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");


        //Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
        //                                                        .and(col("year").geq(2007)));


        //Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");

        dataset.createOrReplaceTempView("students");

        Dataset<Row> results = spark.sql("select * from students where subject ='French' and year >= 2009");


        results.show();


        spark.close();
    }
}
