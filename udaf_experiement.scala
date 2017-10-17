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
// spark.udf.register("gm", new GeometricMean)
spark.udf.register("GeometricMean",new GeometricMean)

// COMMAND ----------

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GeometricMean2 extends UserDefinedAggregateFunction {
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

// COMMAND ----------



// COMMAND ----------

// Create a DataFrame and Spark SQL Table to query.
import org.apache.spark.sql.functions._

val ids = spark.range(1, 20)
ids.createOrReplaceTempView("ids")
val df = spark.sql("select id, id % 3 as group_id from ids")
df.createOrReplaceTempView("simple")

// COMMAND ----------

display(df)

// COMMAND ----------

df.groupBy($"group_id").agg(GeometricMean($"id"))

// COMMAND ----------

val df = Seq(("O","B",1.0,10),("O","S",1.0,10),("O","B",2.0,5),("O","S",2.0,50),("R","B",1.0,10),("R","S",1.0,10),("R","B",2.0,5),("R","S",2.0,50)).toDF("e","s","p","q")
df.createOrReplaceTempView("df")

// COMMAND ----------

// MAGIC %sql
// MAGIC select e,collect_list(map(p,sum)) from
// MAGIC (
// MAGIC select e, p, sum(q) as sum from df group by e, p
// MAGIC ) group by e

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC select e, first_value(p), sum(q) over (partition by p) as sum from df group by e

// COMMAND ----------

// MAGIC %sql
// MAGIC select e,p, sum(q) over(partition by e,p ), map(p,sum(q) over(partition by e,p )) from df

// COMMAND ----------

select *, row_number()  from df

// COMMAND ----------

// MAGIC %sql
// MAGIC select e,collect_list(map(p,sum(q) over(partition by e,p ))) over(partition by e) as coll_map, row_number() OVER (PARTITION BY e)   from df

// COMMAND ----------

// MAGIC %sql
// MAGIC select e, first_value(collect_list(map(p,sum(q) over(partition by e,p ))) over(partition by e)) over (partition by e)  from df 

// COMMAND ----------



// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC select e, map(p,first_value(sum(q) over(partition by e,p ))) from df

// COMMAND ----------

// MAGIC %sql
// MAGIC select first(e, map(p,sum(q) over(partition by e,p ))) from df

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC from pyspark.sql.window import Window
// MAGIC df = sc.parallelize([
// MAGIC     ("a", None), ("a", 1), ("a", -1), ("b", 3)
// MAGIC ]).toDF(["k", "v"])
// MAGIC w = Window().partitionBy("k").orderBy("v")
// MAGIC 
// MAGIC display(df.select(col("k"), first("v").over(w).alias("fv")))

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC df.registerTempTable("df")
// MAGIC 
// MAGIC display(sqlContext.sql("""
// MAGIC     SELECT k, first_value(v, TRUE) OVER (PARTITION BY k)
// MAGIC     FROM df"""))

// COMMAND ----------

// MAGIC %python
// MAGIC display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Use a group_by statement and call the UDAF.
// MAGIC select group_id, gm(id) from simple group by group_id

// COMMAND ----------

// Show the geometric mean of values of column "id".
df.groupBy("group_id").agg(GeometricMean(col("id")).as("GeometricMean")).show()

// COMMAND ----------

import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import scala.util.{Try, Success, Failure}

val getTimestamp: (String => Option[Timestamp]) = s => s match {
  case "" => None
  case _ => {
    val format = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss.SSS")
    Try(new Timestamp(format.parse(s).getTime)) match {
      case Success(t) => Some(t)
      case Failure(_) => None
    }    
  }
}



// COMMAND ----------

spark.udf.register("getTimestamp",getTimestamp)

// COMMAND ----------

// val getTimestampUDF = udf(getTimestamp)
val tdf = Seq((1L, "05/26/2016 01:01:01.601"), (2L, "#$@#@#")).toDF("id", "dts")
val tts = getTimestamp($"dts")
tdf.withColumn("ts", tts).show(2, false)

// COMMAND ----------


