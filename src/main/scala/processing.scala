//object processing {
package processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object processing {

  def main(args: Array[String]): Unit = {

    // ===============================
    // 1) Create Spark Session
    // ===============================
    val spark = SparkSession.builder()
      .appName("Classroom Recommendation Engine")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ===============================
    // 2) Read Streaming Data from Kafka
    // ===============================
    val rawKafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rooms_stream")
      .option("startingOffsets", "latest")
      .load()

    // Convert Kafka value from bytes → string
    val stringDF = rawKafkaStream.selectExpr("CAST(value AS STRING) as json")

    // ===============================
    // 3) Define Schema of Incoming JSON
    // ===============================
    val schema = new StructType()
      .add("room_id", IntegerType)
      .add("room_name", StringType)
      .add("building", StringType)
      .add("college", StringType)
      .add("course_name", StringType)
      .add("doctor_name", StringType)
      .add("expected_students", IntegerType)
      .add("capacity", IntegerType)
      .add("historical_frequency", DoubleType)
      .add("is_reserved", BooleanType)

    // Parse JSON into real columns
    val parsed = stringDF.select(from_json(col("json"), schema).as("data"))
      .select("data.*")

    // ===============================
    // 4) Data Cleaning
    // ===============================
    val cleaned = parsed
      .na.drop()                       // remove null rows
      .dropDuplicates()                // remove duplicate records
      .filter($"capacity" > 0)         // valid capacity
      .filter($"expected_students" > 0)

    // ===============================
    // 5) Recommendation Algorithm
    // ===============================
    // (في المشروع الحقيقي الدكتور رح يدخل هدول من واجهة، هون نحط قيم تجريبية)

    val doctorCourse = "Algorithms"
    val doctorStudents = 70

    val recommended = cleaned
      .filter($"course_name" === doctorCourse)        // نفس المادة
      .filter($"capacity" >= doctorStudents)          // تسع عدد الطلاب
      .filter($"is_reserved" === false)               // القاعة مش محجوزة
      .withColumn(
        "score",
        $"historical_frequency" * 0.6 -              // استخدام القاعة سابقًا
          abs($"capacity" - doctorStudents) * 0.4      // أقرب سعة مطابقة للدكتور
      )
      .orderBy(desc("score"))
      .limit(3)

    // ===============================
    // 6) Output Stream to Console (Temporary)
    // ===============================
    val query = recommended.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}
