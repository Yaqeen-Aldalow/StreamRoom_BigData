package consumer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object consumer{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RoomBookingConsumer")
      .master("local[*]")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/ROOMDB")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    val schema = new StructType()
      .add("source_type", StringType)
      .add("booking_id", StringType)
      .add("onetime_id", StringType)
      .add("section_id", StringType)
      .add("classroom_id", StringType)
      .add("course_id", StringType)
      .add("professor_id", StringType)
      .add("professor_id", StringType)
      .add("date", StringType)
      .add("start_time", StringType)
      .add("end_time", StringType)
      .add("students", IntegerType)
      .add("fixed_students", IntegerType)
      .add("room_number", StringType)
      .add("capacity", IntegerType)
      .add("department", StringType)
      .add("college_id", StringType)
      .add("day_schedule", StringType)
      .add("duration_hours", StringType)
      .add("booking_type", StringType)
      .add("ingestion_timestamp", StringType)
      .add("ingestion_date", StringType)

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "room_requests")
      .option("startingOffsets", "latest")
      .load()

    val parsed = kafkaStream
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    val query = parsed.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        batchDF.filter(col("source_type") === "fixed_booking")
          .dropDuplicates("booking_id")
          .write
          .format("mongo")
          .option("uri", "mongodb://127.0.0.1/ROOMDB.fixed_booking")
          .mode("append")
          .save()

        batchDF.filter(col("source_type") === "one_time_booking")
          .dropDuplicates("onetime_id")
          .write
          .format("mongo")
          .option("uri", "mongodb://127.0.0.1/ROOMDB.one_time_booking")
          .mode("append")
          .save()

        batchDF.filter(col("source_type") === "courses")
          .dropDuplicates("course_id")
          .write
          .format("mongo")
          .option("uri", "mongodb://127.0.0.1/ROOMDB.courses")
          .mode("append")
          .save()

        batchDF.filter(col("source_type") === "professors")
          .dropDuplicates("professor_id")
          .write
          .format("mongo")
          .option("uri", "mongodb://127.0.0.1/ROOMDB.professors")
          .mode("append")
          .save()

        batchDF.filter(col("source_type") === "sections")
          .dropDuplicates("section_id")
          .write
          .format("mongo")
          .option("uri", "mongodb://127.0.0.1/ROOMDB.sections")
          .mode("append")
          .save()

        batchDF.filter(col("source_type") === "classroom")
          .dropDuplicates("classroom_id")
          .write
          .format("mongo")
          .option("uri", "mongodb://127.0.0.1/ROOMDB.classrooms")
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    query.awaitTermination()
 // test edit
 }
}
