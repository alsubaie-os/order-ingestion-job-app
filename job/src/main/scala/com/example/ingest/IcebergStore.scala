package com.example.ingest

import org.apache.iceberg.{Snapshot, Table}
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object IcebergStore {

  val Catalog = "local"

  val RunTsKey = "run_ts"
  val KafkaOffsetsKey = "kafka.offsets"
  val RowsRejectedKey = "rows_rejected"

  def qualifiedName(table: String): String = s"$Catalog.$table"

  def ensureTable(spark: SparkSession, table: String): Unit = {
    val namespace = table.substring(0, table.lastIndexOf('.'))
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")

    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  order_id        STRING,
         |  customer_id     STRING,
         |  amount          DECIMAL(18,3),
         |  currency        STRING,
         |  status          STRING,
         |  event_time      TIMESTAMP,
         |  kafka_timestamp TIMESTAMP,
         |  kafka_partition INT,
         |  kafka_offset    BIGINT,
         |  run_ts          TIMESTAMP
         |)
         |USING iceberg
         |PARTITIONED BY (days(event_time))
         |TBLPROPERTIES ('format-version' = '2')""".stripMargin)
  }

  def loadTable(spark: SparkSession, table: String): Table = Spark3Util.loadIcebergTable(spark, table)


  def committedOffsets(table: Table, topic: String): Map[Int, Long] = {
    table.refresh()
    Option(table.currentSnapshot()) match {
      case None =>
        Map.empty

      case Some(current) =>
        latestWithOffsets(table, current) match {
          case Some(snapshot) =>
            OffsetJson.read(topic, snapshot.summary().get(KafkaOffsetsKey))
          case None =>
            if (hasData(table, current)) {
              throw new IllegalStateException(
                s"Table has data but no snapshot with '$KafkaOffsetsKey'. Refusing to guess where to start.")
            }
            Map.empty
        }
    }
  }

  // Uses the snapshot's row count; when that is missing, looks for data files instead of assuming empty.
  private def hasData(table: Table, snapshot: Snapshot): Boolean =
    Option(snapshot.summary().get("total-records")) match {
      case Some(rows) => rows.toLong > 0
      case None =>
        val files = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()
        try files.iterator().hasNext
        finally files.close()
    }

  @tailrec
  private def latestWithOffsets(table: Table, snapshot: Snapshot): Option[Snapshot] =
    if (snapshot.summary().containsKey(KafkaOffsetsKey)) {
      Some(snapshot)
    } else {
      Option(snapshot.parentId()).flatMap(id => Option(table.snapshot(id.longValue()))) match {
        case Some(parent) => latestWithOffsets(table, parent)
        case None         => None
      }
    }

  def commitWithoutData(table: Table, properties: Map[String, String]): Unit = {
    table.refresh()
    val append = table.newAppend()
    properties.foreach { case (key, value) => append.set(key, value) }
    append.commit()
  }
}
