// Databricks notebook source
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GeometricMean extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("product", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}
val GeometricMean = new GeometricMean()

spark.udf.register("GeometricMean",GeometricMean)


// COMMAND ----------

import org.apache.spark.sql.functions._
val ids = spark.range(1, 20)
ids.createOrReplaceTempView("ids")
val df = spark.sql("select id, id % 3 as group_id from ids")
df.createOrReplaceTempView("simple")

// COMMAND ----------

display(df.groupBy("group_id").agg(GeometricMean(col("id")).as("GeometricMean")))

// COMMAND ----------

// MAGIC %sql
// MAGIC select group_id, GeometricMean(id) as GeometricMean from simple group by group_id

// COMMAND ----------

val df = Seq(("O","B",1.0,10),("O","S",1.0,10),("O","B",2.0,5),("O","S",2.0,50),("R","B",1.0,10),("R","S",1.0,10),("R","B",2.0,5),("R","S",2.0,50)).toDF("e","s","p","q")
df.createOrReplaceTempView("df")

// COMMAND ----------

// MAGIC %sql
// MAGIC select e,collect_list(map(p,sum)) from (select e, p, sum(q) as sum from df group by e, p) group by e

// COMMAND ----------

import org.apache.spark.sql.functions._
display(df.groupBy("e","p").agg(sum("q").as("new_sum")).groupBy("e").agg(collect_list(map($"p",$"new_sum"))))

// COMMAND ----------

df.groupBy("e","p").agg(sum("q").as("new_sum")).groupBy("e").agg(collect_list(map($"p",$"new_sum"))).explain()
