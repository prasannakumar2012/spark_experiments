// Databricks notebook source
// MAGIC %md 
// MAGIC # Demonstrate how a collection of data can be easily queried and modified idempotently using coarse grained transactions.
// MAGIC 
// MAGIC #### 1. Batch writes
// MAGIC #### 2. Stream writes
// MAGIC #### 3. Simulate write transaction failures
// MAGIC #### 4. Access via `SQL`
// MAGIC #### 5. Directories and Files
// MAGIC #### 6. Joins and Upserts

// COMMAND ----------

// MAGIC %md
// MAGIC ###Before Databricks delta
// MAGIC ![before](https://s3.us-east-2.amazonaws.com/databricks-roy/before_delta.jpeg)

// COMMAND ----------

// MAGIC %md
// MAGIC ###After Databricks delta
// MAGIC ![after](https://s3.us-east-2.amazonaws.com/databricks-roy/after_delta.jpeg)

// COMMAND ----------

// MAGIC %md ##Setup

// COMMAND ----------

val clsObj = classOf[org.apache.spark.sql.execution.datasources.DataSource$]
val dSource = clsObj.getField("MODULE$").get(clsObj).asInstanceOf[org.apache.spark.sql.execution.datasources.DataSource$]
val field = clsObj.getDeclaredField("backwardCompatibilityMap")
field.setAccessible(true)
val oldMap = field.get(dSource).asInstanceOf[Map[String, String]]
val newMap = oldMap + ("delta" -> classOf[com.databricks.sql.transaction.tahoe.ReservoirDataSource].getName)
field.set(dSource, newMap)

// COMMAND ----------

import com.databricks.sql.transaction.tahoe._

case class Event(
  date: java.sql.Date, 
  eventType: String, 
  location: String,
  deviceId: Long
)

def createEvents(count: Int, date: String, eventType: String) = (1 to count).toList.map(id => Event(java.sql.Date.valueOf(date), eventType, "NY", (id%5)+1))

dbutils.fs.rm("/mnt/roy/hg", true)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 1. Batch writes:
// MAGIC ### Write 1,000 events for `10/1`

// COMMAND ----------

createEvents(1000, "2017-10-1", "batch-insert")
  .toDF()
  .write
  .format("delta")
  .save("/mnt/roy/hg")

// COMMAND ----------

// MAGIC %md ### Start a streaming query to count events by date

// COMMAND ----------

display(
  spark
  .readStream
  .format("delta")
  .load("/mnt/roy/hg")
  .groupBy("date", "eventType")
  .count
  .orderBy("date")
)

// COMMAND ----------

// MAGIC %md 
// MAGIC ###Append 5,000 events for `10/5`

// COMMAND ----------

createEvents(5000, "2017-10-5", "batch-append")
  .toDF()
  .write
  .format("delta")
  .mode("append")
  .save("/mnt/roy/hg")

// COMMAND ----------

// MAGIC %md ###...wait for `Cmd 9` to show `10/1` and `10/5`...

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 2. Stream writes
// MAGIC ### Write stream 10,000 events for `10/10`

// COMMAND ----------

val df = createEvents(10000, "2017-10-10", "stream").toDF()
val schema = df.schema
df.write
  .format("json")
  .mode("overwrite")
  .save("/mnt/roy/eventsStream")

val streamDF = spark
  .readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .json("/mnt/roy/eventsStream")

// COMMAND ----------

streamDF
  .writeStream
  .format("delta")
  .option("path", "/mnt/roy/hg")
  .option("checkpointLocation", "/tmp/roy/hg/checkpoint/")
  .start()

// COMMAND ----------

// MAGIC %md ###...wait for `Cmd 9` to show `10/1`, `10/5`, and `10/10`...

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 3. Simulate write transaction failure
// MAGIC 
// MAGIC ### Try to append Events with *bad* date format
// MAGIC ### Append 30,000 events for `10/30`

// COMMAND ----------

val badDF = (
              (1 to 15000).toList.map(_ => ("2017-31-10", "batch-tx", "NY")) ++ (1 to 15000).toList.map(_ => ("2017-10-31", "batch-tx", "NY"))
            ).toDF("date", "eventType", "location")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Failure

// COMMAND ----------

badDF
  .write
  .format("delta")
  .mode("append")
  .save("/mnt/roy/hg")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Fix the date format

// COMMAND ----------

import org.apache.spark.sql.functions.{coalesce, to_date}

val goodDF = badDF.withColumn("date", 
                              coalesce($"date".cast("date"), to_date($"date", "yyyy-dd-MM"))
                             )

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Success!!

// COMMAND ----------

goodDF
  .write
  .format("delta")
  .mode("append")
  .save("/mnt/roy/hg")

// COMMAND ----------

// MAGIC %md ###...wait for `Cmd 9` to show `10/1`, `10/5`, `10/10`, and `10/30`...

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 4. Access delta via `SQL`

// COMMAND ----------

// MAGIC %sql SELECT count(*), date, eventType FROM tahoe.`/mnt/roy/hg` GROUP BY date, eventType ORDER BY date ASC

// COMMAND ----------

// MAGIC %sql CREATE OR REPLACE VIEW roy_events AS SELECT * FROM tahoe.`/mnt/roy/hg`

// COMMAND ----------

// MAGIC %sql SELECT count(*), date, eventType FROM roy_events GROUP BY date, eventType ORDER BY date ASC

// COMMAND ----------

// MAGIC %sql OPTIMIZE '/mnt/roy/hg'

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 5. Directories and Files

// COMMAND ----------

// MAGIC %fs ls /mnt/roy/hg

// COMMAND ----------

display(spark.read.parquet("/mnt/roy/hg/part-00000-12a3f13b-5ee6-4ead-9a66-e314b42f4c8b-c000.gz.parquet"))

// COMMAND ----------

// MAGIC %fs ls /mnt/roy/hg/_delta_log/

// COMMAND ----------

// MAGIC %fs head /mnt/roy/hg/_delta_log/00000000000000000000.json

// COMMAND ----------

display(spark.read.parquet("/mnt/roy/hg/_delta_log/00000000000000000010.checkpoint.parquet"))

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 6. Joins and Upserts

// COMMAND ----------

case class Device(
  deviceId: Long,
  deviceType: String
)

(1 to 10).toList.map(id => Device(id, s"device_$id"))
  .toDF()
  .write
  .format("parquet")
  .mode("overwrite")
  .saveAsTable("devices")

// COMMAND ----------

// MAGIC %sql SELECT * FROM devices

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT count(e.*), d.deviceType 
// MAGIC FROM roy_events e, devices d
// MAGIC WHERE d.deviceId = e.deviceId 
// MAGIC GROUP BY d.deviceType

// COMMAND ----------

(1 to 10)
  .toList.map(id => Event(java.sql.Date.valueOf("2017-10-1"), "upsert", "CA", id))
  .toDF
  .createOrReplaceTempView("temp_table")

// COMMAND ----------

// MAGIC %sql SELECT * FROM temp_table

// COMMAND ----------

// MAGIC %sql SELECT count(*), e.location FROM tahoe.`/mnt/roy/hg` e GROUP BY e.location

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO tahoe.`/mnt/roy/hg` AS target
// MAGIC USING temp_table
// MAGIC ON temp_table.deviceId == target.deviceId AND temp_table.date == target.date
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET target.date = temp_table.date, target.eventType = temp_table.eventType, target.location = temp_table.location, target.deviceId = temp_table.deviceId
// MAGIC WHEN NOT MATCHED 
// MAGIC   THEN INSERT (target.date, target.eventType, target.location, target.deviceId) VALUES (temp_table.date, temp_table.eventType, temp_table.location, temp_table.deviceId)

// COMMAND ----------

// MAGIC %sql SELECT count(*), e.location FROM tahoe.`/mnt/roy/hg` e GROUP BY e.location

// COMMAND ----------

// MAGIC %md
// MAGIC #TL;DR
// MAGIC 
// MAGIC 
// MAGIC ![delta](https://databricks.com/wp-content/uploads/2017/10/Screen-Shot-2017-10-25-at-01.15.08.png)
// MAGIC 
// MAGIC Reference:
// MAGIC https://databricks.com/blog/2017/10/25/databricks-delta-a-unified-management-system-for-real-time-big-data.html

// COMMAND ----------


