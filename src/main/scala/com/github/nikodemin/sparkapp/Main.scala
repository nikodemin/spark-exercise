package com.github.nikodemin.sparkapp

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
      .show()

    println("")
    initialDf
  }
}
