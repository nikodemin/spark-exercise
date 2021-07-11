package com.github.nikodemin.sparkapp.aggregator

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.annotation.tailrec

case class SessionAggregator(sessionMaxThreshold: Long)
  extends Aggregator[Long, List[(Long, Long)], List[(String, Long)]] {
  override def zero: List[(Long, Long)] = List.empty

  override def reduce(acc: List[(Long, Long)], timestamp: Long): List[(Long, Long)] = mergeIntervals(
    acc.map { case (start, end) =>
      if (timestamp >= start - sessionMaxThreshold && timestamp <= end + sessionMaxThreshold) {
        if (timestamp > end) (start, timestamp)
        else if (timestamp < start) (timestamp, end)
        else (start, end)
      }
      else (start, end)
    } ++ List((timestamp, timestamp))
  )

  override def merge(acc1: List[(Long, Long)], acc2: List[(Long, Long)]): List[(Long, Long)] =
    mergeIntervals(acc1 ++ acc2)

  override def finish(reduction: List[(Long, Long)]): List[(String, Long)] = reduction.map { case (start, end) =>
    val formatTimestamp: Long => String = timestamp => LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
      ZoneId.systemDefault).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    (s"${formatTimestamp(start)} - ${formatTimestamp(end)}", end - start)
  }

  override def bufferEncoder: Encoder[List[(Long, Long)]] = Encoders.product

  override def outputEncoder: Encoder[List[(String, Long)]] = Encoders.product

  @tailrec
  private def mergeIntervals(list: List[(Long, Long)], acc: List[(Long, Long)] = List.empty): List[(Long, Long)] =
    list match {
      case Nil => acc
      case ::(head, tail) =>
        val (mergeableElements, otherElements) = tail.partition { case (start, end) =>
          head._1 - sessionMaxThreshold <= end ||
            head._2 + sessionMaxThreshold >= start ||
            (head._1 >= start && head._2 <= end)
        }

        val mergedList = mergeableElements.+:(head)

        mergeIntervals(otherElements, acc.+:((mergedList.minBy(_._1)._1, mergedList.maxBy(_._2)._2)))
    }
}
