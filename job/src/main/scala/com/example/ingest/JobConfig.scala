package com.example.ingest

import java.time.Instant

final case class JobConfig(
    bootstrapServers: String,
    topic: String,
    warehouse: String,
    table: String,
    runTs: String
) {
  require(
    table.matches(JobConfig.TableNamePattern),
    s"--table must look like namespace.table, got '$table'"
  )
}

object JobConfig {

  private val TableNamePattern: String =
    "[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*"

  def parse(args: Array[String]): JobConfig = {
    require(args.length % 2 == 0, "Arguments must come in pairs: --name value")

    val options = args
      .grouped(2)
      .map { case Array(name, value) =>
        require(name.startsWith("--"), s"Expected --name, got '$name'")
        name.stripPrefix("--") -> value
      }
      .toMap

    def required(name: String): String = {
      val value = options
        .getOrElse(
          name,
          throw new IllegalArgumentException(
            s"Missing required argument --$name"
          )
        )
        .trim
      require(value.nonEmpty, s"Argument --$name must not be blank")
      value
    }

    JobConfig(
      bootstrapServers = required("bootstrap-servers"),
      topic = required("topic"),
      warehouse = required("warehouse"),
      table = required("table"),
      runTs = Instant.parse(required("run-ts")).toString
    )
  }
}