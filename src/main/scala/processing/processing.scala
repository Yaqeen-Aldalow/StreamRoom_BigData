package processing

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.sqrt
import scala.io.StdIn
import scala.util.Try

// Bloom Filter imports
import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels

object HybridRecommendationApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hybrid Room Recommendation")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/StreamRoom")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val classrooms = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.classrooms")
      .load()
      .select("classroom_id", "room_number", "capacity", "college_id")
      .withColumn("capacity", col("capacity").cast(IntegerType))

    val fixedBookings = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.fixed_booking")
      .load()
      .select("classroom_id", "date", "start_time", "end_time", "students")
      .withColumn("students", col("students").cast(IntegerType))

    val oneTimeBookings = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.one_time_booking")
      .load()
      .select("classroom_id", "date", "start_time", "end_time", "students")
      .withColumn("students", col("students").cast(IntegerType))

    val courses = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.courses")
      .load()
      .select("course_id", "department", "fixed_students")
      .withColumnRenamed("fixed_students", "students")
      .withColumnRenamed("course_id", "course_name")

    val events = fixedBookings.union(oneTimeBookings).filter(col("classroom_id").isNotNull)

    val usage = events.groupBy("classroom_id").agg(count("*").alias("usage_count"))

    val window = Window.partitionBy("classroom_id").orderBy(desc("usage_count"))

    val collaborativeScore = usage.withColumn(
      "collab_score",
      col("usage_count") / max("usage_count").over(window)
    )

    val eventsWithTs = events
      .withColumn("start_ts", to_timestamp(concat_ws(" ", col("date"), col("start_time")), "dd/MM/yyyy HH:mm"))
      .withColumn("end_ts", to_timestamp(concat_ws(" ", col("date"), col("end_time")), "dd/MM/yyyy HH:mm"))

    // Improved contentScore: choose closest capacity
    def contentScore(dept: String, students: Int): DataFrame = {
      classrooms
        .filter(col("capacity") >= students)
        .withColumn("capacity_diff", col("capacity") - students)
        .withColumn(
          "content_score",
          when(lower(col("college_id")) === dept.toLowerCase, 1.0).otherwise(0.6)
        )
        .orderBy(col("capacity_diff")) // الأقرب أولاً
    }

    def addAvailability(baseRooms: DataFrame, requestedDate: String, requestedStart: String, requestedEnd: String): DataFrame = {
      val reqStartTs = to_timestamp(lit(s"$requestedDate $requestedStart"), "dd/MM/yyyy HH:mm")
      val reqEndTs = to_timestamp(lit(s"$requestedDate $requestedEnd"), "dd/MM/yyyy HH:mm")

      val conflictingClassrooms = eventsWithTs
        .filter(to_date(col("date"), "dd/MM/yyyy") === to_date(lit(requestedDate), "dd/MM/yyyy"))
        .filter(col("start_ts") < reqEndTs && col("end_ts") > reqStartTs)
        .select("classroom_id").distinct()
        .withColumn("has_conflict", lit(1))

      baseRooms
        .join(conflictingClassrooms, Seq("classroom_id"), "left")
        .withColumn("availability_score", when(col("has_conflict").isNull, 1.0).otherwise(0.0))
        .drop("has_conflict")
    }

    def recommendRoomsForRequest(dept: String, students: Int, requestedDate: String, requestedStart: String, requestedEnd: String): DataFrame = {
      val base = contentScore(dept, students)
      val withAvail = addAvailability(base, requestedDate, requestedStart, requestedEnd)

      withAvail
        .join(collaborativeScore, Seq("classroom_id"), "left")
        .na.fill(0, Seq("usage_count", "collab_score"))
        .withColumn("final_score",
          col("content_score") * 0.3 +
            col("collab_score") * 0.3 +
            col("availability_score") * 0.4
        )
        .filter(col("availability_score") === 1.0)   // ✅ فقط القاعات الفاضية
        .orderBy(col("capacity_diff"))               // ✅ الأقرب لعدد الطلاب
        .select(
          col("classroom_id"), col("room_number"), col("capacity"), col("college_id"),
          col("content_score"), col("collab_score"), col("availability_score"), col("final_score")
        )
        .limit(3)                                    // ✅ يعرض فقط 3 فاضيات
    }

    // === Bloom Filter for duplicate prevention ===
    val bloomFilter = BloomFilter.create[CharSequence](
      Funnels.stringFunnel(java.nio.charset.StandardCharsets.UTF_8),
      10000
    )

    def isDuplicateBooking(key: String): Boolean = {
      if (bloomFilter.mightContain(key)) true
      else {
        bloomFilter.put(key)
        false
      }
    }

    // === Validation Helpers ===
    def isValidDate(date: String): Boolean = {
      val parts = date.split("/").map(_.trim)
      if (parts.length != 3) return false
      val dayOpt = Try(parts(0).toInt).toOption
      val monthOpt = Try(parts(1).toInt).toOption
      val yearOpt = Try(parts(2).toInt).toOption

      (dayOpt, monthOpt, yearOpt) match {
        case (Some(day), Some(month), Some(year)) =>
          if (month < 1 || month > 12) return false
          val maxDays = month match {
            case 1 | 3 | 5 | 7 | 8 | 10 | 12 => 31
            case 4 | 6 | 9 | 11             => 30
            case 2 =>
              if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) 29 else 28
          }
          day >= 1 && day <= maxDays
        case _ => false
      }
    }

    def isValidTime(time: String): Boolean = {
      val parts = time.split(":").map(_.trim)
      if (parts.length != 2) return false
      val hourOpt = Try(parts(0).toInt).toOption
      val minuteOpt = Try(parts(1).toInt).toOption

      (hourOpt, minuteOpt) match {
        case (Some(hour), Some(minute)) =>
          hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59
        case _ => false
      }
    }

    def padDate(date: String): String = {
      val parts = date.split("/").map(_.trim)
      f"${parts(0).toInt}%02d/${parts(1).toInt}%02d/${parts(2)}"
    }

    def padTime(t: String): String = {
      val parts = t.split(":").map(_.trim)
      val hour = f"${parts(0).toInt}%02d"
      val minute = f"${parts(1).toInt}%02d"
      s"$hour:$minute"
    }

    // === LIVE DEMO MODE ===
    println("\n=== LIVE DEMO MODE (Doctor Input) ===")
    var continueLoop = true

    while (continueLoop) {
      println("\nEnter Department (or type exit):")
      val deptLine = StdIn.readLine()
      val dept = Option(deptLine).map(_.trim).getOrElse("")

      if (dept.isEmpty || dept.equalsIgnoreCase("exit")) {
        println("Live demo finished.")
        continueLoop = false
      } else {
        println("Enter number of students:")
        val studentsInput = StdIn.readLine().trim
        val maybeStudents = Try(studentsInput.toInt).toOption

        if (maybeStudents.isEmpty || maybeStudents.get <= 0) {
          println("⚠️ Invalid number of students. Please enter a positive integer.")
        } else {
          val students = maybeStudents.get

          println("Enter date (dd/MM/yyyy):")
          val dateInput = StdIn.readLine().trim
          if (!isValidDate(dateInput)) {
            println("⚠️ Invalid date. Please enter a real date in dd/MM/yyyy format.")
          } else {
            val date = padDate(dateInput)

            println("Enter start time (HH:mm):")
            val startInput = StdIn.readLine().trim
            if (!isValidTime(startInput)) {
              println("⚠️ Invalid start time. Please enter a valid time (HH:mm, 24-hour format).")
            } else {
              val start = padTime(startInput)

              println("Enter end time (HH:mm):")
              val endInput = StdIn.readLine().trim
              if (!isValidTime(endInput)) {
                println("⚠️ Invalid end time. Please enter a valid time (HH:mm, 24-hour format).")
              } else {
                val end = padTime(endInput)

                println("Enter booking type (fixed/one_time):")
                val bookingInput = StdIn.readLine().trim.toLowerCase
                val bookingType = bookingInput match {
                  case "f" | "fixed"    => "fixed"
                  case "o" | "one_time" => "one_time"
                  case other =>
                    println(s"⚠️ Unknown booking type: $other. Defaulting to confirmed_bookings.")
                    "confirmed_bookings"
                }

                val liveResult =
                  recommendRoomsForRequest(dept, students, date, start, end)
                    .withColumn("requested_department", lit(dept))
                    .withColumn("requested_students", lit(students))
                    .withColumn("requested_date", lit(date))
                    .withColumn("requested_start", lit(start))
                    .withColumn("requested_end", lit(end))
                    .withColumn("booking_type", lit(bookingType))

                if (liveResult.isEmpty) {
                  println("⚠️ No suitable classroom available for this request.")
                } else {
                  // Duplicate check using Bloom Filter
                  val bookingKey = s"$dept-$students-$date-$start-$end"
                  if (isDuplicateBooking(bookingKey)) {
                    println("⚠️ This booking already exists in confirmed_bookings.")
                  } else {
                    println("===== Live Recommendation Result =====")
                    liveResult.show(false)

                    val targetCollection = "StreamRoom.confirmed_bookings"

                    liveResult.write
                      .format("mongo")
                      .option("uri", s"mongodb://127.0.0.1/$targetCollection")
                      .mode("append")
                      .save()

                    println(s"✅ Booking saved to confirmed_bookings")
                  }
                }
              }
            }
          }
        }
      }
    }

    // Close Spark session when finished
    spark.stop()
  }
}
