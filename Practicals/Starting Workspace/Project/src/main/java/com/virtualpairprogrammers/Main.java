package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        Double result = myRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("\nResult = " + result);

        System.out.println("======================\n");


        List<Integer> intInputData = new ArrayList<>();
        intInputData.add(35);
        intInputData.add(12);
        intInputData.add(90);
        intInputData.add(20);

        JavaRDD<Integer> myIntRdd = sc.parallelize(intInputData);

        JavaRDD<Double> sqrtRdd = myIntRdd.map( value -> Math.sqrt(value));

        sqrtRdd.foreach( value -> System.out.println(value));
        System.out.println("======================\n");
        sqrtRdd.collect().forEach( System.out::println);

        JavaRDD<Integer> myCountRdd = myRdd.map( value -> 1);
        int count = myCountRdd.reduce((val1,val2) -> val1 + val2);

        System.out.println("======================\n");
        System.out.println("Count = " + count);

        System.out.println("======================\n");

        JavaRDD<IntegerWithSquareRoot> sqrtRdd2 = myIntRdd.map( value -> new IntegerWithSquareRoot(value));

        sqrtRdd2.foreach(value -> System.out.println("Square root for " + value.getOriginalNumber() + " is " + value.getSquareRoot()));

        System.out.println("======================\n");

        Tuple2<Integer, Double> myTupleValue = new Tuple2<> (9, 3.0);

        sc.close();
    }
}
