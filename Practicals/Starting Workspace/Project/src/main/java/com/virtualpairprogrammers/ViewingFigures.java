package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = true;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// TODO - over to you!

		CalculatePoints calculatePoints = new CalculatePoints();

		// Ingest Chapters (chapterId,courseId)
		//JavaRDD<String> chaptersRdd = sc.textFile("src/main/resources/viewing figures/chapters.csv");

		JavaRDD<String> chaptersRdd = chapterData.map( data -> data._1 + "," + data._2);

		JavaPairRDD<Integer, Long> courseChapterCountRdd = chaptersRdd.mapToPair(rawdata -> new Tuple2<>(Integer.valueOf(rawdata.split(",")[1]), 1L));

		// Course chapter count.
		JavaPairRDD<Integer, Long> courseTotalChaptersRdd = courseChapterCountRdd.reduceByKey((value1, value2) -> value1 + value2 );


		// userId,chapterId,dateAndTime
		//JavaRDD<String> viewsRdd = sc.textFile("src/main/resources/viewing figures/views-1.csv");

		JavaRDD<String> viewsRdd = viewData.map( data -> data._1 + "," + data._2);

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
		
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
