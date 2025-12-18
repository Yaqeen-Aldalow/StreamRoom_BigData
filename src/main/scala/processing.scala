package processing

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import scala.math.sqrt

object HybridRecommendationApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hybrid Room Recommendation")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/StreamRoom")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // ============================
    // 1) قراءة البيانات من MongoDB
    // ============================

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

    // دمج كل الأحداث للاستفادة في الusage
    val events = fixedBookings
      .union(oneTimeBookings)
      .filter(col("classroom_id").isNotNull)

    // ============================
    // 2) حساب usage / collaborative score
    // ============================

    val usage = events
      .groupBy("classroom_id")
      .agg(count("*").alias("usage_count"))

    val window = Window.orderBy(desc("usage_count"))

    val collaborativeScore = usage
      .withColumn(
        "collab_score",
        col("usage_count") / max("usage_count").over(window)
      )

    // تجهيز timestamps للحجوزات (عشان التعارض)
    val eventsWithTs = events
      .withColumn(
        "start_ts",
        to_timestamp(concat_ws(" ", col("date"), col("start_time")), "dd/MM/yyyy HH:mm")
      )
      .withColumn(
        "end_ts",
        to_timestamp(concat_ws(" ", col("date"), col("end_time")), "dd/MM/yyyy HH:mm")
      )

    // ============================
    // 3) دالة حساب content score
    // ============================

    def contentScore(dept: String, students: Int): DataFrame = {
      classrooms
        .filter(col("capacity") >= students)
        .withColumn(
          "content_score",
          when(lower(col("college_id")) === dept.toLowerCase, 1.0)
            .otherwise(0.6)
        )
    }

    // ============================
    // 4) دالة تجنب التعارض بالوقت
    // ============================

    def addAvailability(
                         baseRooms: DataFrame,
                         requestedDate: String,
                         requestedStart: String,
                         requestedEnd: String
                       ): DataFrame = {

      val reqStartTs = to_timestamp(lit(s"$requestedDate $requestedStart"), "dd/MM/yyyy HH:mm")
      val reqEndTs   = to_timestamp(lit(s"$requestedDate $requestedEnd"), "dd/MM/yyyy HH:mm")

      val conflictingClassrooms = eventsWithTs
        .filter(
          to_date(col("date"), "dd/MM/yyyy") === to_date(lit(requestedDate), "dd/MM/yyyy")
        )
        .filter(
          col("start_ts") < reqEndTs && col("end_ts") > reqStartTs
        )
        .select("classroom_id")
        .distinct()
        .withColumn("has_conflict", lit(1))

      baseRooms
        .join(conflictingClassrooms, Seq("classroom_id"), "left")
        .withColumn(
          "availability_score",
          when(col("has_conflict").isNull, 1.0).otherwise(0.0)
        )
        .drop("has_conflict")
    }

    // ============================
    // 5) دالة التوصية الأساسية لطلب واحد
    // ============================

    def recommendRoomsForRequest(
                                  dept: String,
                                  students: Int,
                                  requestedDate: String,
                                  requestedStart: String,
                                  requestedEnd: String
                                ): DataFrame = {

      val base = contentScore(dept, students)

      val withAvail = addAvailability(base, requestedDate, requestedStart, requestedEnd)

      withAvail
        .join(collaborativeScore, Seq("classroom_id"), "left")
        .na.fill(0, Seq("usage_count", "collab_score"))
        .withColumn(
          "final_score",
          col("content_score") * 0.3 +
            col("collab_score") * 0.3 +
            col("availability_score") * 0.4      // نعطي أولوية لكونها متاحة فعلاً
        )
        .filter(col("availability_score") === 1.0) // نستبعد القاعات المتعارضة
        .orderBy(desc("final_score"))
        .select(
          col("classroom_id"),
          col("room_number"),
          col("capacity"),
          col("college_id"),
          col("content_score"),
          col("collab_score"),
          col("availability_score"),
          col("final_score")
        )
        .limit(3)
    }

    // ============================
    // 6) مثال (أ) – بداية الفصل: نطبق على كل كورس
    // ============================

    val courseRows = courses.select("course_name", "department", "students").collect()

    // نفترض إن الدكتور بده كل الكورسات في تاريخ معيّن (مثلاً أول أسبوع)
    val defaultDate   = "02/09/2025"
    val defaultStart  = "08:00"
    val defaultEnd    = "09:00"

    val resultsForCourses = courseRows.map { row =>
      val courseName = row.getString(0)
      val dept       = row.getString(1)
      val students   = row.getInt(2)

      recommendRoomsForRequest(dept, students, defaultDate, defaultStart, defaultEnd)
        .withColumn("course_name", lit(courseName))
        .withColumn("requested_department", lit(dept))
        .withColumn("requested_students", lit(students))
        .withColumn("requested_date", lit(defaultDate))
        .withColumn("requested_start", lit(defaultStart))
        .withColumn("requested_end", lit(defaultEnd))
    }.reduce(_ union _)

    println("===== Best 3 rooms per course (start of semester) =====")
    resultsForCourses.show(false)

    // ============================
    // 7) مثال (ب) – نص الفصل: طلب واحد (امتحان/إيفنت)
    // ============================

    val midDept        = "Mathematics"
    val midStudents    = 40
    val midDate        = "31/12/2025"
    val midStart       = "13:00"
    val midEnd         = "14:00"

    val midResults = recommendRoomsForRequest(midDept, midStudents, midDate, midStart, midEnd)
      .withColumn("requested_department", lit(midDept))
      .withColumn("requested_students", lit(midStudents))
      .withColumn("requested_date", lit(midDate))
      .withColumn("requested_start", lit(midStart))
      .withColumn("requested_end", lit(midEnd))

    println("===== Best 3 rooms for mid-semester event/exam =====")
    midResults.show(false)

    // ============================
    // 8) تقييم بسيط (اختياري)
    // ============================

    val eval = resultsForCourses.withColumn(
      "error",
      col("final_score") - col("collab_score")
    )

    val mse = eval.select(avg(pow(col("error"), 2))).first().getDouble(0)
    val rmse = sqrt(mse)

    println("===================================")
    println(s"MSE  = $mse")
    println(s"RMSE = $rmse")
    println("===================================")

    spark.stop()
  }
}