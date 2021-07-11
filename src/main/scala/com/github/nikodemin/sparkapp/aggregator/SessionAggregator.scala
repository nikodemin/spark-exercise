package com.github.nikodemin.sparkapp.aggregator

import com.github.nikodemin.sparkapp.aggregator.SessionAggregator.Record
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

/**
 * Aggregator for grouping views by sessions
 *
 * @param betweenSessionMaxThreshold max duration between two consequent session that are treatet as one session
 * @param afkMinThreshold            min duration after which session considering as ended
 * @param timestampColumnName        name of view timestamp column in row
 * @param durationColumnName         name of feed viewing duration column in row
 * @param positionColumnName         name of feed position column in row
 */
case class SessionAggregator(
                              betweenSessionMaxThreshold: FiniteDuration,
                              afkMinThreshold: FiniteDuration,
                              timestampColumnName: String,
                              durationColumnName: String,
                              positionColumnName: String
                            )
  extends Aggregator[Row, List[Record], List[Record]] {
  override def zero: List[Record] = List.empty

  override def reduce(acc: List[Record], row: Row): List[Record] = {
    val timestamp = row.getAs[Long](timestampColumnName)
    val duration = Math.min(row.getAs[Long](durationColumnName), afkMinThreshold.toMillis)
    val position = row.getAs[Long](positionColumnName)
    mergeWithElement(acc, Record(timestamp, timestamp + duration, position, position))
  }

  override def merge(acc: List[Record], acc2: List[Record]): List[Record] = mergeIntervals(acc, acc2)

  override def finish(reduction: List[Record]): List[Record] = reduction

  override def bufferEncoder: Encoder[List[Record]] = Encoders.product

  override def outputEncoder: Encoder[List[Record]] = Encoders.product

  @tailrec
  private def mergeIntervals(list: List[Record], acc: List[Record]): List[Record] =
    list match {
      case Nil => acc
      case head :: tail => mergeIntervals(tail, mergeWithElement(acc, head))
    }

  private def mergeWithElement(list: List[Record], element: Record): List[Record] = {
    val (mergeableElements, otherElements) = list.partition { case Record(start, end, _, _) =>
      (element.sessionStartTime - betweenSessionMaxThreshold.toMillis <= end && element.sessionEndTime >= end) ||
        (element.sessionEndTime + betweenSessionMaxThreshold.toMillis >= start && element.sessionStartTime <= start) ||
        (element.sessionStartTime >= start && element.sessionEndTime <= end)
    }

    val mergedList = mergeableElements.+:(element)

    val sessionStart = mergedList.minBy(_.sessionStartTime).sessionStartTime
    val sessionEnd = mergedList.maxBy(_.sessionEndTime).sessionEndTime
    val minPos = mergedList.minBy(_.minPostPosition).minPostPosition
    val maxPos = mergedList.maxBy(_.maxPostPosition).maxPostPosition

    otherElements.+:(Record(sessionStart, sessionEnd, minPos, maxPos))
  }
}

object SessionAggregator {

  case class Record(sessionStartTime: Long, sessionEndTime: Long, minPostPosition: Long, maxPostPosition: Long)

}
