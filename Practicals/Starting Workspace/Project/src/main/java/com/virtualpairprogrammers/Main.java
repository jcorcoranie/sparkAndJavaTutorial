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

        //dataset.show();

        long numberOfRows = dataset.count();

        //System.out.println(" There are " + numberOfRows + " in the dataset");

        Row firstRow = dataset.first();

        String subject = firstRow.get(2).toString();
        String subjectAgain = firstRow.getAs("subject");

        System.out.println(subject);
        System.out.println(subjectAgain);

        spark.close();
    }
}
