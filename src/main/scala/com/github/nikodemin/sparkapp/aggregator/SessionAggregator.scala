package com.github.nikodemin.sparkapp.aggregator

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.annotation.tailrec

case class SessionAggregator(sessionMaxThreshold: Long)
  extends Aggregator[Long, List[(Long, Long)], List[(String, Long)]] {
  override def zero: List[(Long, Long)] = List.empty

  override def reduce(acc: List[(Long, Long)], timestamp: Long): List[(Long, Long)] =
    mergeWithElement(acc, (timestamp, timestamp))

  override def merge(acc: List[(Long, Long)], acc2: List[(Long, Long)]): List[(Long, Long)] = mergeIntervals(acc, acc2)

  override def finish(reduction: List[(Long, Long)]): List[(String, Long)] = reduction.map { case (start, end) =>
    val formatTimestamp: Long => String = timestamp => LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
      ZoneId.systemDefault).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    (s"${formatTimestamp(start)} - ${formatTimestamp(end)}", end - start)
  }

  override def bufferEncoder: Encoder[List[(Long, Long)]] = Encoders.product

  override def outputEncoder: Encoder[List[(String, Long)]] = Encoders.product

  @tailrec
  private def mergeIntervals(list: List[(Long, Long)], acc: List[(Long, Long)]): List[(Long, Long)] =
    list match {
      case Nil => acc
      case head :: tail => mergeIntervals(tail, mergeWithElement(acc, head))
    }

  private def mergeWithElement(list: List[(Long, Long)], element: (Long, Long)): List[(Long, Long)] = {
    val (mergeableElements, otherElements) = list.partition { case (start, end) =>
      (element._1 - sessionMaxThreshold <= end && element._2 >= end) ||
        (element._2 + sessionMaxThreshold >= start && element._1 <= start) ||
        (element._1 >= start && element._2 <= end)
    }

    val mergedList = mergeableElements.+:(element)
    otherElements.+:((mergedList.minBy(_._1)._1, mergedList.maxBy(_._2)._2))
  }
}
