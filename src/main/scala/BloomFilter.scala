import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import com.google.common.hash.{BloomFilter, Funnels}
import java.nio.charset.StandardCharsets

object consumer {
  // فلتر واحد جاهز ل توبك roomrequest الذي يحتوي ال6 فايلات
  val bloom = BloomFilter.create(
    Funnels.stringFunnel(StandardCharsets.UTF_8),
    100000,
    0.01 // نسبة الخطا المسموح فيها
  )
  def filterWithBloom(df: DataFrame, col: String): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val filteredRDD = df.rdd.filter { row =>
      val value = row.getAs[String](col)
      val isNew = !bloom.mightContain(value)
      if (isNew) bloom.put(value)
      isNew
    }

    spark.createDataFrame(filteredRDD, df.schema)
  }

  // ===== دوال معالجة حسب نوع البيانات =====

  def processFixed(df: DataFrame): Unit = {
    val cleaned = filterWithBloom(df, "booking_id")
    println("=== FIXED BOOKINGS ===")
    cleaned.show(false)
  }

  def processOneTime(df: DataFrame): Unit = {
    val cleaned = filterWithBloom(df, "onetime_id")
    println("=== ONE TIME BOOKINGS ===")
    cleaned.show(false)
  }

  def processCourses(df: DataFrame): Unit = {
    val cleaned = filterWithBloom(df, "course_id")
    println("=== COURSES ===")
    cleaned.show(false)
  }

  def processSections(df: DataFrame): Unit = {
    val cleaned = filterWithBloom(df, "section_id")
    println("=== SECTIONS ===")
    cleaned.show(false)
  }

  def processProfessors(df: DataFrame): Unit = {
    val cleaned = filterWithBloom(df, "professor_id")
    println("=== PROFESSORS ===")
    cleaned.show(false)
  }

  def processClassrooms(df: DataFrame): Unit = {
    val cleaned = filterWithBloom(df, "classroom_id")
    println("=== CLASSROOMS ===")
    cleaned.show(false)
  }

  // ===== MAIN =====
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "room_requests")
      .load()

    val parsed = kafkaDF
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json($"json", spark.read.json(Seq("""{}""").toDS).schema).as("data"))
      .select("data.*")

    parsed.writeStream
      .foreachBatch { (batch: DataFrame, id: Long) =>

        val fixed = batch.filter($"source_type" === "fixed_booking")
        val oneTime = batch.filter($"source_type" === "one_time_booking")
        val courses = batch.filter($"source_type" === "courses")
        val sections = batch.filter($"source_type" === "sections")
        val professors = batch.filter($"source_type" === "professors")
        val classrooms = batch.filter($"source_type" === "classroom")

        if (!fixed.isEmpty) processFixed(fixed)
        if (!oneTime.isEmpty) processOneTime(oneTime)
        if (!courses.isEmpty) processCourses(courses)
        if (!sections.isEmpty) processSections(sections)
        if (!professors.isEmpty) processProfessors(professors)
        if (!classrooms.isEmpty) processClassrooms(classrooms)
      }
      .start()
      .awaitTermination()
  }
}