package processing

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import java.nio.charset.StandardCharsets

object HybridRecommendationApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hybrid Room Recommendation")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/StreamRoom")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ================= READ DATA =================
    val classrooms = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.classrooms")
      .load()
      .select("classroom_id", "room_number", "capacity", "college_id")
      .withColumn("capacity", col("capacity").cast(IntegerType))

    val fixedBookings = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.fixed_booking")
      .load()
      .select("classroom_id", "date", "start_time", "end_time", "students")

    val oneTimeBookings = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.one_time_booking")
      .load()
      .select("classroom_id", "date", "start_time", "end_time", "students")

    val events = fixedBookings.union(oneTimeBookings)

    val usage = events.groupBy("classroom_id")
      .agg(count("*").alias("usage_count"))

    val window = Window.orderBy(desc("usage_count"))

    val collaborativeScore = usage.withColumn(
      "collab_score",
      col("usage_count") / max("usage_count").over(window)
    )

    val eventsWithTs = events
      .withColumn("start_ts",
        to_timestamp(concat_ws(" ", col("date"), col("start_time")), "dd/MM/yyyy HH:mm"))
      .withColumn("end_ts",
        to_timestamp(concat_ws(" ", col("date"), col("end_time")), "dd/MM/yyyy HH:mm"))

    // ================= HELPERS =================
    def padDate(date: String): String = {
      val p = date.split("/")
      f"${p(0).toInt}%02d/${p(1).toInt}%02d/${p(2)}"
    }

    def padTime(t: String): String = {
      val p = t.split(":")
      f"${p(0).toInt}%02d:${p(1).toInt}%02d"
    }

    def contentScore(dept: String, students: Int, course: String): DataFrame = {
      classrooms
        .filter(col("capacity") >= students)
        .withColumn("capacity_diff", col("capacity") - students)
        .withColumn(
          "content_score",
          when(lower(col("college_id")) === dept.toLowerCase, 1.0).otherwise(0.6)
        )
        // إذا المادة فيها كلمة "lab" نعطي وزن إضافي
        .withColumn(
          "course_score",
          when(lower(lit(course)).contains("lab"), 1.0).otherwise(0.0)
        )
    }

    def addAvailability(base: DataFrame, d: String, s: String, e: String): DataFrame = {
      val startTs = to_timestamp(lit(s"$d $s"), "dd/MM/yyyy HH:mm")
      val endTs   = to_timestamp(lit(s"$d $e"), "dd/MM/yyyy HH:mm")

      val conflicts = eventsWithTs
        .filter(col("start_ts") < endTs && col("end_ts") > startTs)
        .select("classroom_id").distinct()
        .withColumn("busy", lit(1))

      base.join(conflicts, Seq("classroom_id"), "left")
        .withColumn("availability_score",
          when(col("busy").isNull, 1.0).otherwise(0.0))
        .drop("busy")
    }

    def recommendRooms(dept: String, students: Int, d: String, s: String, e: String, doctor: String, course: String, existing: DataFrame): DataFrame = {
      val base = contentScore(dept, students, course)
      val free = addAvailability(base, d, s, e)

      // حساب تفضيل الدكتور من confirmed_bookings
      val doctorBookings = existing
        .filter(lower(col("doctor_name")) === doctor.toLowerCase)
        .groupBy("classroom_id")
        .agg(count("*").alias("doctor_usage"))

      val maxDoctorUsage = if (doctorBookings.isEmpty) 1 else doctorBookings.agg(max("doctor_usage")).first().getLong(0)

      val doctorPrefScore = doctorBookings.withColumn(
        "prof_pref_score",
        col("doctor_usage") / lit(maxDoctorUsage)
      )

      free.join(collaborativeScore, Seq("classroom_id"), "left")
        .join(doctorPrefScore, Seq("classroom_id"), "left")
        .na.fill(0)
        .withColumn("final_score",
          col("content_score") * 0.25 +
            col("course_score") * 0.15 +
            col("collab_score") * 0.25 +
            col("availability_score") * 0.2 +
            col("prof_pref_score") * 0.15
        )
        .filter(col("availability_score") === 1)
        .orderBy(desc("final_score"))
        .limit(3)
    }

    // ================= BLOOM FILTER =================
    val funnel = Funnels.stringFunnel(StandardCharsets.UTF_8)
    val bloomFilter: BloomFilter[String] =
      BloomFilter.create[String](funnel, 100000, 0.01d)

    val existing = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.confirmed_bookings")
      .load()

    if (existing.columns.contains("booking_id")) {
      existing.select("booking_id").collect().foreach { row =>
        val v = row.getAs[String]("booking_id")
        if (v != null) bloomFilter.put(v)
      }
    }

    def bookingKey(dept: String, students: Int, date: String, start: String, end: String,
                   doctor: String, course: String, classroomId: String): String = {
      s"$dept|$students|$date|$start|$end|$doctor|$course|$classroomId"
    }

    // ================= LIVE DEMO =================
    println("\n=== LIVE DEMO MODE ===")

    var run = true
    while (run) {

      println("Enter Department (or exit):")
      val dept = StdIn.readLine().trim
      if (dept.equalsIgnoreCase("exit")) run = false
      else {

        println("Enter students:")
        val students = StdIn.readLine().trim.toInt

        println("Enter date (dd/MM/yyyy):")
        val rawDate = StdIn.readLine().trim
        val date = padDate(rawDate)

        println("Enter start (HH:mm):")
        val rawStart = StdIn.readLine().trim
        val start = padTime(rawStart)

        println("Enter end (HH:mm):")
        val rawEnd = StdIn.readLine().trim
        val end = padTime(rawEnd)

        println("Doctor name:")
        val doctor = StdIn.readLine().trim

        println("Course name:")
        val course = StdIn.readLine().trim

        val result = recommendRooms(dept, students, date, start, end, doctor, course, existing)

        if (result.isEmpty) {
          println("⚠️ No available rooms")
        } else {
          result.show(false)

          println("Choose classroom_id:")
          val chosen = StdIn.readLine().trim

          val key = bookingKey(dept, students, date, start, end, doctor, course, chosen)

          if (bloomFilter.mightContain(key)) {
            println("⚠️ Duplicate request, will not be saved")
          } else {
            bloomFilter.put(key)

            val booking = result.filter(col("classroom_id") === chosen)
              .withColumn("doctor_name", lit(doctor))
              .withColumn("course_name", lit(course))
              .withColumn("requested_date", lit(date))
              .withColumn("requested_start", lit(start))
              .withColumn("requested_end", lit(end))
              .withColumn("requested_students", lit(students))
              .withColumn("requested_department", lit(dept))
              .withColumn("booking_id", lit(key))

            booking.write
              .format("mongo")
              .option("uri", "mongodb://127.0.0.1/StreamRoom.confirmed_bookings")
              .mode("append")
              .save()

            println("✅ Booking saved")
          }
        }
      }
    }

    spark.stop()
  }
}
