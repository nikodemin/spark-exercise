package com.github.nikodemin.sparkapp

import com.github.nikodemin.sparkapp.aggregator.SessionAggregator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.concurrent.duration._

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my first spark app")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("durationMs", LongType)
      .add("owners", new StructType()
        .add("group", ArrayType(LongType, containsNull = false))
        .add("user", ArrayType(LongType, containsNull = false))
      )
      .add("platform", StringType)
      .add("position", LongType)
      .add("resources", new StructType()
        .add("GROUP_PHOTO", ArrayType(LongType, containsNull = false))
        .add("MOVIE", ArrayType(LongType, containsNull = false))
        .add("POST", ArrayType(LongType, containsNull = false))
        .add("USER_PHOTO", ArrayType(LongType, containsNull = false)),
      )
      .add("timestamp", LongType)
      .add("userId", LongType)

    val initialDf = spark.read
      .schema(schema)
      .json("src/main/resources/feeds_show.json")

    println("Schema and first 3 elements of initial data frame:")
    initialDf.printSchema
    initialDf.show(3)

    val viewsAndUsersByPlatformDf = initialDf
      .groupBy("platform")
      .agg(count("userId").as("views"), countDistinct("userId").as("unique_users"))
      .select("platform", "views", "unique_users")
      .cache

    println("Count of views and users grouped by platforms")
    viewsAndUsersByPlatformDf.show

    println("Summary count of views and users")
    viewsAndUsersByPlatformDf.select(sum("views").as("summary_views"),
      sum("unique_users").as("summary_users")).show

    println("Daily unique authors and content")
    initialDf.withColumn("date", from_unixtime(expr("timestamp/1000"), "yyyy/MM/dd"))
      .groupBy("date")
      .agg(
        countDistinct("owners.user").as("unique_users"),
        countDistinct("owners.group").as("unique_groups"),
        countDistinct("resources.GROUP_PHOTO").as("unique_group_photos"),
        countDistinct("resources.MOVIE").as("unique_movies"),
        countDistinct("resources.POST").as("unique_posts"),
        countDistinct("resources.USER_PHOTO").as("unique_user_photos")
      ).select("date", "unique_users", "unique_groups", "unique_group_photos",
      "unique_movies", "unique_posts", "unique_user_photos")
      .show

    println("Grouping views by sessions and calculating duration of each session")
    val betweenSessionMaxThreshold = 0.seconds
    val afkMinThreshold = 10.minutes
    val sessionAggregator = SessionAggregator(betweenSessionMaxThreshold, afkMinThreshold,
      "timestamp", "durationMs", "position")

    val sessionDf = initialDf.groupBy("userId")
      .agg(sessionAggregator.toColumn.as("session_agg"))
      .select(col("userId"), explode(col("session_agg")).as("session"))
      .select(
        col("userId"),
        col("session.sessionStartTime").cast(LongType).as("session_start_time"),
        col("session.sessionEndTime").cast(LongType).as("session_end_time"),
        col("session.minPostPosition").cast(LongType).as("min_post_pos"),
        col("session.maxPostPosition").cast(LongType).as("max_post_pos")
      )
      .withColumn("date_repr", concat(
        from_unixtime(expr("session_start_time/1000"), "yyyy-MM-dd hh:mm:ss"),
        lit(" - "),
        from_unixtime(expr("session_end_time/1000"), "yyyy-MM-dd hh:mm:ss"),
      ))
      .withColumn("session_duration", expr("(session_end_time - session_start_time)/1000"))
      .orderBy(col("session_duration").desc)
      .cache

    sessionDf.printSchema
    sessionDf.show(20, truncate = false)

    sessionDf.agg(
      count("session_duration").as("session_count"),
      avg("session_duration"),
      expr("AVG(max_post_pos - min_post_pos)").as("viewing_depth")
    ).show

    val userIdsDf = List.iterate(0, 50000)(_ + 1).toDF("userId")

    println("Views grouped by users")
    initialDf
      .join(userIdsDf, "userId")
      .groupBy("userId")
      .agg(count("*").as("views"))
      .show
  }
}
