
package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.google.common.hash.{BloomFilter, Funnels}
import java.nio.charset.StandardCharsets

object HybridRecommendationApp {

  def main(args: Array[String]): Unit = {

    // 1) Spark Session
    val spark = SparkSession.builder()
      .appName("Hybrid Room Recommendation")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/StreamRoom")
      .getOrCreate()

    import spark.implicits._

    // 2) Load only essential data
    val classrooms = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.classrooms")
      .load()
      .select("classroom_id", "capacity", "college_id")
      .withColumn("capacity", col("capacity").cast(IntegerType))

    val bookings = spark.read.format("mongo")
      .option("uri", "mongodb://127.0.0.1/StreamRoom.confirmed_bookings")
      .load()
      .select("booking_id", "classroom_id", "students")

    // 3) Collaborative usage score
    val usage = bookings.groupBy("classroom_id").agg(count("*").as("usage_count"))
    val maxUsage = usage.select(max("usage_count")).as[Long].headOption.getOrElse(1L)
    val collaborativeScore = usage.withColumn("collab_score", $"usage_count" / maxUsage)

    // 4) Bloom Filter (avoid duplicates)
    val bloom = BloomFilter.create[String](
      Funnels.stringFunnel(StandardCharsets.UTF_8),
      100000,
      0.01
    )
    bookings.select("booking_id").as[String].collect().foreach { id =>
      if (id != null) bloom.put(id)
    }

    // 5) Content score
    def contentScore(dept: String, students: Int, course: String): DataFrame = {
      classrooms
        .withColumn("capacity_clean",
          when($"capacity".isNull || $"capacity" <= 0, lit(null).cast(IntegerType))
            .otherwise($"capacity"))
        .withColumn("capacity_score",
          when($"capacity_clean".isNull, 0.05)
            .otherwise(
              when($"capacity_clean" < students, 0.0)
                .otherwise(1.0 / ($"capacity_clean" - students + 1))
            )
        )
        .withColumn("content_score",
          when(lower($"college_id") === dept.toLowerCase, 1.0).otherwise(0.6))
        .withColumn("course_score",
          when(lower(lit(course)).contains("lab"), 1.0).otherwise(0.0))
    }

    // 6) Metrics (MSE/RMSE)
    def printCapacityMetrics(df: DataFrame, students: Int): Unit = {
      val errs = df
        .withColumn("capacity_clean",
          when($"capacity".isNull || $"capacity" <= 0, 0).otherwise($"capacity"))
        .withColumn("error_sq", pow($"capacity_clean" - students, 2))
        .agg(avg($"error_sq").as("mse"))
        .withColumn("rmse", sqrt($"mse"))

      val metrics = errs.select("mse", "rmse").first()
      println(f"📊 MSE = ${metrics.getDouble(0)}%.4f, RMSE = ${metrics.getDouble(1)}%.4f")
    }
    printCapacityMetrics(scored, students)
  }
}
