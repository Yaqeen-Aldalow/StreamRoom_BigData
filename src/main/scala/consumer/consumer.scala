package consumer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import BloomFilterUtils._

object consumer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RoomBookingConsumer")
      .master("local[*]")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/StreamRoom")
      .getOrCreate()

    import spark.implicits._

    val schema = new StructType()
      .add("source_type", StringType)
      .add("booking_id", StringType)
      .add("onetime_id", StringType)
      .add("section_id", StringType)
      .add("classroom_id", StringType)
      .add("course_id", StringType)
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

        val fixedBloom = loadExistingIds(spark, "fixed_booking", "booking_id")
        val fixedClean = filterNew(batchDF.filter($"source_type" === "fixed_booking"), "booking_id", fixedBloom)
        fixedClean.write.format("mongo").mode("append")
          .option("uri", "mongodb://127.0.0.1/StreamRoom.fixed_booking").save()

        val oneBloom = loadExistingIds(spark, "one_time_booking", "onetime_id")
        val oneClean = filterNew(batchDF.filter($"source_type" === "one_time_booking"), "onetime_id", oneBloom)
        oneClean.write.format("mongo").mode("append")
          .option("uri", "mongodb://127.0.0.1/StreamRoom.one_time_booking").save()

        val courseBloom = loadExistingIds(spark, "courses", "course_id")
        val courseClean = filterNew(batchDF.filter($"source_type" === "courses"), "course_id", courseBloom)
        courseClean.write.format("mongo").mode("append")
          .option("uri", "mongodb://127.0.0.1/StreamRoom.courses").save()

        val profBloom = loadExistingIds(spark, "professors", "professor_id")
        val profClean = filterNew(batchDF.filter($"source_type" === "professors"), "professor_id", profBloom)
        profClean.write.format("mongo").mode("append")
          .option("uri", "mongodb://127.0.0.1/StreamRoom.professors").save()

        val secBloom = loadExistingIds(spark, "sections", "section_id")
        val secClean = filterNew(batchDF.filter($"source_type" === "sections"), "section_id", secBloom)
        secClean.write.format("mongo").mode("append")
          .option("uri", "mongodb://127.0.0.1/StreamRoom.sections").save()

        val clsBloom = loadExistingIds(spark, "classrooms", "classroom_id")
        val clsClean = filterNew(batchDF.filter($"source_type" === "classroom"), "classroom_id", clsBloom)
        clsClean.write.format("mongo").mode("append")
          .option("uri", "mongodb://127.0.0.1/StreamRoom.classrooms").save()
      }
      .start()

    query.awaitTermination()
  }
}
