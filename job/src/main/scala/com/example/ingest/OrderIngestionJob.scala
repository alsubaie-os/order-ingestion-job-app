package com.example.ingest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object OrderIngestionJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.parse(args)
    val ordersTable = IcebergStore.qualifiedName(config.table)

    // Configure the filesystem Iceberg catalog and UTC timestamps.
    val catalog = s"spark.sql.catalog.${IcebergStore.Catalog}"
    val spark = SparkSession
      .builder()
      .appName("order-ingestion")
      .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
      .config(s"$catalog.type", "hadoop")
      .config(s"$catalog.warehouse", config.warehouse)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      IcebergStore.ensureTable(spark, ordersTable)
      val table = IcebergStore.loadTable(spark, ordersTable)

      // Resume from saved offsets and freeze Kafka's end offsets for this run.
      val committed = IcebergStore.committedOffsets(table, config.topic)

      val bounds = KafkaOffsets.fetch(config.bootstrapServers, config.topic)
      val plan = OffsetPlanner.plan(
        config.topic,
        committed,
        bounds.earliest,
        bounds.latest
      )
      println(
        s"Run ${config.runTs} into $ordersTable: ${plan.totalMessages} new messages, " +
          s"${plan.startingOffsetsJson} -> ${plan.endingOffsetsJson}"
      )

      val runProperties = Map(
        IcebergStore.RunTsKey -> config.runTs,
        IcebergStore.KafkaOffsetsKey -> plan.endingOffsetsJson
      )

      if (plan.isEmpty) {
        // Record empty runs in snapshot metadata.
        IcebergStore.commitWithoutData(
          table,
          runProperties + (IcebergStore.RowsRejectedKey -> "0")
        )
        println(s"Run ${config.runTs}: nothing new")
      } else {
        // Read each partition's fixed [start, end) range.
        val messages = spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", config.bootstrapServers)
          .option("subscribe", config.topic)
          .option("startingOffsets", plan.startingOffsetsJson)
          .option("endingOffsets", plan.endingOffsetsJson)
          .load()

        val parsed = OrderTransform.parse(messages).cache()
        val rejectedCount =
          parsed.filter(col("reject_reason").isNotNull).count()
        val validCount = parsed.filter(col("reject_reason").isNull).count()
        val properties =
          runProperties + (IcebergStore.RowsRejectedKey -> rejectedCount.toString)

        if (validCount > 0) {
          // Commit rows and offsets together so failed writes can be retried safely.
          OrderTransform
            .validOrders(parsed, config.runTs)
            .writeTo(ordersTable)
            .options(properties.map { case (key, value) =>
              s"snapshot-property.$key" -> value
            })
            .append()
        } else {
          // Advance offsets even when every message is rejected.
          IcebergStore.commitWithoutData(table, properties)
        }
        println(
          s"Run ${config.runTs}: $validCount loaded, $rejectedCount rejected"
        )
      }

      spark.sql(s"REFRESH TABLE $ordersTable")

      spark
        .sql(s"""SELECT committed_at, snapshot_id,
           |       summary['run_ts'] AS run_ts,
           |       coalesce(summary['added-records'], '0') AS added,
           |       summary['total-records'] AS total,
           |       summary['kafka.offsets'] AS kafka_offsets
           |FROM $ordersTable.snapshots
           |ORDER BY committed_at""".stripMargin)
        .show(false)
    } finally {
      spark.stop()
    }
  }
}
