package processing

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
    import spark.implicits._

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

    // usage per classroom
    val usage = events.groupBy("classroom_id")
      .agg(count("*").alias("usage_count"))

    // Avoid WindowExec warning: compute max usage as a scalar
    val maxUsage = usage.agg(max("usage_count")).as[Long].collect().headOption.getOrElse(1L)
    val collaborativeScore = usage.withColumn(
      "collab_score",
      col("usage_count") / lit(maxUsage)
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

    // Content features + capacity handling (including unknown capacities)
    def contentScore(dept: String, students: Int, course: String): DataFrame = {
      val deptLower = dept.toLowerCase
      classrooms
        // normalize capacity (unknown or invalid -> null)
        .withColumn("capacity_clean",
          when(col("capacity").isNull || col("capacity") <= 0, lit(null).cast(IntegerType))
            .otherwise(col("capacity")))
        // difference if capacity known
        .withColumn("capacity_diff",
          when(col("capacity_clean").isNotNull, col("capacity_clean") - lit(students)))
        // higher score when capacity is just-enough; unknown capacity gets tiny score; too-small rooms get 0
        .withColumn("capacity_score",
          when(col("capacity_clean").isNull, lit(0.05))
            .otherwise(
              when(col("capacity_clean") < lit(students), lit(0.0))
                .otherwise(
                  exp(- (col("capacity_clean") - lit(students)) / 20.0)
                )
            )
        )

        .withColumn(
          "content_score",
          when(lower(col("college_id")) === deptLower, 1.0).otherwise(0.6)
        )
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

    def recommendRooms(
                        dept: String,
                        students: Int,
                        d: String,
                        s: String,
                        e: String,
                        doctor: String,
                        course: String,
                        existing: DataFrame
                      ): DataFrame = {
      val base = contentScore(dept, students, course)
      val free = addAvailability(base, d, s, e)

      // doctor preference
      val doctorBookings = existing
        .filter(lower(col("doctor_name")) === doctor.toLowerCase)
        .groupBy("classroom_id")
        .agg(count("*").alias("doctor_usage"))

      val docCount = doctorBookings.count()
      val maxDoctorUsage = if (docCount == 0) 1L
      else doctorBookings.agg(max("doctor_usage")).as[Long].collect().head

      val doctorPrefScore = doctorBookings.withColumn(
        "prof_pref_score",
        col("doctor_usage") / lit(maxDoctorUsage)
      )

      val scored = free
        .join(collaborativeScore, Seq("classroom_id"), "left")
        .join(doctorPrefScore, Seq("classroom_id"), "left")
        .na.fill(0, Seq("usage_count", "collab_score", "doctor_usage", "prof_pref_score"))
        // updated weights: emphasize availability and capacity fit
        .withColumn("final_score",
          col("content_score")      * 0.20 +
            col("course_score")       * 0.10 +
            col("collab_score")       * 0.20 +
            col("availability_score") * 0.25 +
            col("prof_pref_score")    * 0.05 +
            col("capacity_score")     * 0.20
        )
        // must be available; capacity must be adequate or unknown
        .filter(col("availability_score") === 1.0 &&
          (col("capacity_clean") >= lit(students) || col("capacity_clean").isNull))

      scored.orderBy(desc("final_score")).limit(3)
    }

    // ================= BLOOM FILTER =================
    val funnel = Funnels.stringFunnel(StandardCharsets.UTF_8)
    val bloomFilter: BloomFilter[String] =
      BloomFilter.create[String](funnel, 100000, 0.01d)

    val existing = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.confirmed_bookings")
      .load()

    if (existing.columns.contains("booking_id")) {
      existing.select("booking_id").as[String].collect().foreach { v =>
        if (v != null) bloomFilter.put(v)
      }
    }

    def bookingKey(dept: String, students: Int, date: String, start: String, end: String,
                   doctor: String, course: String, classroomId: String): String = {
      s"$dept|$students|$date|$start|$end|$doctor|$course|$classroomId"
    }

    // ================= METRICS (MSE/RMSE) =================
    def printCapacityMetrics(df: DataFrame, students: Int): Unit = {
      val errs = df
        .withColumn("capacity_clean",
          when(col("capacity").isNull || col("capacity") <= 0, lit(0)).otherwise(col("capacity")))
        .withColumn("error_sq", pow(col("capacity_clean") - lit(students), 2))
        .agg(avg(col("error_sq")).alias("mse"))
        .withColumn("rmse", sqrt(col("mse")))

      val metrics = errs.select("mse", "rmse").first()
      val mseVal = metrics.getDouble(0)
      val rmseVal = metrics.getDouble(1)
      println(f"📊 Capacity fit -> MSE = $mseVal%.4f, RMSE = $rmseVal%.4f")
    }

    // ================= INPUT VALIDATION =================
    def validateDate(input: String): Option[String] = {
      try {
        val parts = input.split("/")
        if (parts.length != 3) return None
        val day = parts(0).toInt
        val month = parts(1).toInt
        val year = parts(2).toInt
        if (day >= 1 && day <= 30 && month >= 1 && month <= 12) {
          Some(f"$day%02d/$month%02d/$year")
        } else None
      } catch { case _: Exception => None }
    }

    def validateTime(input: String): Option[String] = {
      try {
        val parts = input.split(":")
        if (parts.length != 2) return None
        val hour = parts(0).toInt
        val minute = parts(1).toInt
        if (hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
          Some(f"$hour%02d:$minute%02d")
        } else None
      } catch { case _: Exception => None }
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

        var date: String = ""
        do {
          println("Enter date (dd/MM/yyyy):")
          val rawDate = StdIn.readLine().trim
          validateDate(rawDate) match {
            case Some(valid) => date = valid
            case None => println("⚠️ Invalid date! Day must be ≤ 30 and month ≤ 12.")
          }
        } while (date.isEmpty)

        var start: String = ""
        do {
          println("Enter start (HH:mm):")
          val rawStart = StdIn.readLine().trim
          validateTime(rawStart) match {
            case Some(valid) => start = valid
            case None => println("⚠️ Invalid time! Hour must be 0–23 and minute 0–59.")
          }
        } while (start.isEmpty)

        var end: String = ""
        do {
          println("Enter end (HH:mm):")
          val rawEnd = StdIn.readLine().trim
          validateTime(rawEnd) match {
            case Some(valid) => end = valid
            case None => println("⚠️ Invalid time! Hour must be 0–23 and minute 0–59.")
          }
        } while (end.isEmpty)

        println("Doctor name:")
        val doctor = StdIn.readLine().trim

        println("Course name:")
        val course = StdIn.readLine().trim

        val result = recommendRooms(dept, students, date, start, end, doctor, course, existing)

        if (result.isEmpty) {
          println("⚠️ No available rooms")
        } else {
          result.show(false)

          // Print capacity metrics for the top recommendations
          printCapacityMetrics(result, students)

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

            // If user typed an ID not in the top-3, warn before saving
            val countChosen = booking.count()
            if (countChosen == 0) {
              println("⚠️ Selected classroom_id is not in the current top-3 recommendations. Booking not saved.")
            } else {
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
    }

    spark.stop()
  }
}
