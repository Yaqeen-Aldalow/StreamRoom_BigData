package processing

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import scala.math.sqrt
import com.google.common.hash.{BloomFilter, Funnels}
import java.nio.charset.StandardCharsets

object RoomRecommendationMongo {

  def main(args: Array[String]): Unit = {

    // =============================
    // 1️⃣ Spark Session + Mongo
    // =============================
    val spark = SparkSession.builder()
      .appName("Hybrid Room Recommendation")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._



    val classrooms = spark.read.format("mongodb")
      .option("database", "ROOMDB")
      .option("collection", "classrooms")
      .load()
      .select(
        col("classroom_id"),
        col("capacity").cast(IntegerType)
      )

    val fixedBookings = spark.read.format("mongodb")
      .option("database", "ROOMDB")
      .option("collection", "fixed_booking")
      .load()
      .select(
        col("classroom_id"),
        col("students").cast(IntegerType)
      )

    val oneTimeBookings = spark.read.format("mongodb")
      .option("database", "ROOMDB")
      .option("collection", "one_time_booking")
      .load()
      .select(
        col("classroom_id"),
        col("students").cast(IntegerType)
      )


    val events = fixedBookings.union(oneTimeBookings)


    val bloom = BloomFilter.create(
      Funnels.stringFunnel(StandardCharsets.UTF_8),
      100000,
      0.01
    )

    events.select("classroom_id").distinct().collect().foreach { row =>
      if (row != null && !row.isNullAt(0)) {
        bloom.put(row.getString(0))
      }
    }

    val isPossiblyBooked = udf { id: String =>
      bloom.mightContain(id)
    }


    def contentScore(students: Int): DataFrame = {
      classrooms
        .filter(col("capacity") >= students)
        .withColumn("content_score", lit(1.0))
    }


    val usage = events
      .groupBy("classroom_id")
      .agg(count("*").alias("usage_count"))

    val maxUsage = usage.agg(max("usage_count")).first().getLong(0)

    val collaborativeScore =
      if (maxUsage == 0) {
        usage.withColumn("collab_score", lit(0.0))
      } else {
        usage.withColumn(
          "collab_score",
          col("usage_count") / lit(maxUsage.toDouble)
        )
      }


    def recommendRooms(students: Int): DataFrame = {

      contentScore(students)
        .join(collaborativeScore, Seq("classroom_id"), "left")
        .na.fill(0.0, Seq("collab_score"))
        .withColumn(
          "availability_score",
          when(isPossiblyBooked(col("classroom_id")), 0.5).otherwise(1.0)
        )
        .withColumn(
          "final_score",
          col("content_score") * 0.5 +
            col("collab_score") * 0.3 +
            col("availability_score") * 0.2
        )
        .orderBy(desc("final_score"))
        .limit(3)
    }

    val requests = Seq(
      30,
      40,
      60
    )

    val results = requests.map { students =>
      recommendRooms(students)
        .withColumn("requested_students", lit(students))
    }.reduce(_ union _)


    val eval = results.withColumn(
      "error",
      col("final_score") - col("collab_score")
    )

    val mse = eval.select(avg(pow(col("error"), 2))).first().getDouble(0)
    val rmse = sqrt(mse)


    println("===================================")
    println(s"📊 MSE  = $mse")
    println(s"📊 RMSE = $rmse")
    println("===================================")

    results.show(false)

    spark.stop()
  }
}