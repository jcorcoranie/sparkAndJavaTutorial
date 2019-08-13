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
        CalculatePoints calculatePoints = new CalculatePoints();

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        // Ingest Chapters (chapterId,courseId)
        JavaRDD<String> chaptersRdd = sc.textFile("src/main/resources/viewing figures/chapters.csv");

        JavaPairRDD<Integer, Long> courseChapterCountRdd = chaptersRdd.mapToPair(rawdata -> new Tuple2<>(Integer.valueOf(rawdata.split(",")[1]), 1L));

        // Course chapter count.
        JavaPairRDD<Integer, Long> courseTotalChaptersRdd = courseChapterCountRdd.reduceByKey((value1, value2) -> value1 + value2 );


        // userId,chapterId,dateAndTime
        JavaRDD<String> viewsRdd = sc.textFile("src/main/resources/viewing figures/views-1.csv");

                                    // userId,chapterId
        JavaPairRDD<String, String> viewsUserIdChapterIdRdd = viewsRdd.mapToPair(rawdata -> new Tuple2<>( rawdata.split(",")[0], rawdata.split(",")[1]));

        // de-dup userId,chapterID
        JavaPairRDD<String, Long> viewsUserIdChapterIdCountRdd = viewsUserIdChapterIdRdd.mapToPair( values -> new Tuple2<>( values._1 + ":" + values._2 , 1L ));

        JavaPairRDD<String, Long> ViewsUserIdChapterIdTotalsRdd = viewsUserIdChapterIdCountRdd.reduceByKey((value1, value2) -> value1 + value2 );

                                    //chapterId, userId
        JavaPairRDD<String, String> viewsChapterIduserIdRdd = ViewsUserIdChapterIdTotalsRdd.mapToPair(rawdata -> new Tuple2<>( rawdata._1.split(":")[1], rawdata._1.split(":")[0]));

                                    //chapterId,courseId
        JavaPairRDD<String, String> chaptersChapterIdCourseId = chaptersRdd.mapToPair(rawdata -> new Tuple2<>(rawdata.split(",")[0], rawdata.split(",")[1]));

                                                // ChapterId, (UserId, CourseId)
        JavaPairRDD<String, Tuple2<String, String>> joinedViewChapterOnChapterId = viewsChapterIduserIdRdd.join(chaptersChapterIdCourseId);

        // Calculate No. of chapters each user viewed for each course.
                                    // UserID, CourseId, 1L
        JavaPairRDD<String, Long> userCoursecountingChapters = joinedViewChapterOnChapterId.mapToPair( chapterPerUserAndCourse -> new Tuple2<>(chapterPerUserAndCourse._2._1 + ":" + chapterPerUserAndCourse._2._2, 1L));

                                    // (UserID:CourseId), Total chapters viewed
        JavaPairRDD<String, Long> userCourseTotalChapters = userCoursecountingChapters.reduceByKey((value1, value2) -> value1 + value2 );

                                    // CourseId, (UserID:Total chapters viewed),
        JavaPairRDD<Integer, Tuple2<String, Long>> CourseUserTotalChapters = userCourseTotalChapters.mapToPair( row -> new Tuple2<>(Integer.valueOf(row._1.split(":")[1]), new Tuple2<>(row._1.split(":")[0], row._2 )));

                                        // CourseId, ((UserID, Total chapters viewed), Total Chapters)
        JavaPairRDD<Integer, Tuple2<Tuple2<String, Long>, Long>> temp = CourseUserTotalChapters.join(courseTotalChaptersRdd);


        JavaPairRDD<Integer, Tuple2<String, Integer>> temp2 = temp.mapToPair( data -> new Tuple2<>( data._1, new Tuple2<>( data._2._1._1, (calculatePoints.calculatePoint((data._2._1._2 * 100d) / data._2._2)))));


        JavaPairRDD<Integer, Integer> courseIdPoints = temp2.mapToPair( data -> new Tuple2<>( data._1, data._2._2));

        JavaPairRDD<Integer, Integer> courseTotals = courseIdPoints.reduceByKey((value1, value2) -> value1 + value2);

        courseTotals.foreach( row -> System.out.println(row));

        //ChapterIduserIdRdd.foreach( row -> System.out.println(row));
        sc.close();
    }



}
