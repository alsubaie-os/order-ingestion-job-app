package com.example.ingest

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._
import scala.util.Try

final case class PartitionRange(partition: Int, from: Long, until: Long) {
  def messageCount: Long = until - from
}

final case class OffsetPlan(topic: String, ranges: Seq[PartitionRange]) {
  def totalMessages: Long = ranges.map(_.messageCount).sum
  def isEmpty: Boolean = totalMessages == 0

  def startingOffsetsJson: String = OffsetJson.write(topic, ranges.map(r => r.partition -> r.from).toMap)

  def endingOffsetsJson: String = OffsetJson.write(topic, ranges.map(r => r.partition -> r.until).toMap)
}

object OffsetPlanner {

  def plan(topic: String, committed: Map[Int, Long], earliest: Map[Int, Long], latest: Map[Int, Long]): OffsetPlan = {
    val ranges = latest.keys.toSeq.sorted.map { partition =>
      val first = earliest(partition)
      val end = latest(partition)

      val from = committed.get(partition) match {
        case None =>
          first

        case Some(next) if next > end =>
          throw new IllegalStateException(
            s"Partition $partition: stored offset $next is beyond the end of the log ($end). " +
              "The topic was probably recreated. Reset the table before running again.")

        case Some(next) if next < first =>
          throw new IllegalStateException(
            s"Partition $partition: offsets $next..${first - 1} were deleted by Kafka retention before they were loaded.")

        case Some(next) =>
          next
      }

      PartitionRange(partition, from, end)
    }
    OffsetPlan(topic, ranges)
  }
}

object OffsetJson {
  private val mapper = new ObjectMapper()

  def write(topic: String, offsets: Map[Int, Long]): String = {
    val partitions = offsets.toSeq.sortBy(_._1).map { case (p, o) => s""""$p":$o""" }
    s"""{"$topic":{${partitions.mkString(",")}}}"""
  }

  // Stored offsets must be {"<topic>":{"<partition>":<offset>,...}} with non-negative integers;
  // anything else stops the run, because a guessed offset would reload or skip messages.
  def read(topic: String, json: String): Map[Int, Long] = {
    def invalid(reason: String) = new IllegalStateException(s"Stored offsets $json are invalid: $reason")

    val topicNode = mapper.readTree(json).get(topic)
    if (topicNode == null || !topicNode.isObject || topicNode.size() == 0) {
      throw invalid(s"no partitions for topic '$topic'")
    }
    topicNode.fields().asScala.map { entry =>
      val partition = Try(entry.getKey.toInt).filter(_ >= 0)
        .getOrElse(throw invalid(s"partition '${entry.getKey}' is not a non-negative integer"))
      val offset = entry.getValue
      if (!offset.isIntegralNumber || !offset.canConvertToLong || offset.asLong() < 0) {
        throw invalid(s"offset $offset for partition $partition is not a non-negative integer")
      }
      partition -> offset.asLong()
    }.toMap
  }
}
