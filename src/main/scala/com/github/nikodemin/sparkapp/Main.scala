package com.github.nikodemin.sparkapp

import com.github.nikodemin.sparkapp.aggregator.SessionAggregator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my first spark app")
      .master("local")
      .getOrCreate()

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

    println("Grouping views by sessions and calculationg duration of each session")
    val sessionMaxThreshold = 20000
    spark.udf.register("sessionAggregate", udaf(SessionAggregator(sessionMaxThreshold)))

    val sessionDf = initialDf.groupBy("userId")
      .agg(("timestamp", "sessionAggregate"))
      .select(col("userId"), explode(col("sessionaggregate(timestamp)")).as("session"))
      .select(
        col("userId"),
        col("session._1").cast(StringType).as("session_duration_string"),
        col("session._2").cast(LongType).as("session_duration")
      )
      .orderBy(col("session_duration").desc)
      .cache

    sessionDf.show(20, truncate = false)

    sessionDf.agg(avg("session_duration"))
  }
}
