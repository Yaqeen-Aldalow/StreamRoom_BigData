package consumer

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.google.common.hash.{BloomFilter, Funnels}
import java.nio.charset.StandardCharsets

object BloomFilterUtils {

  def newFilter(): BloomFilter[String] = {
    val funnel = Funnels.stringFunnel(StandardCharsets.UTF_8)
    BloomFilter.create[String](funnel, 500000, 0.01)
  }

  def loadExistingIds(spark: SparkSession, collection: String, column: String): BloomFilter[String] = {
    val bloom = newFilter()

    val df = spark.read
      .format("mongo")
      .option("uri", s"mongodb://127.0.0.1/StreamRoom.$collection")
      .load()

    if (!df.columns.contains(column)) return bloom

    df.select(column).collect().foreach { row =>
      val v = row.getAs[String](column)
      if (v != null) bloom.put(v)
    }

    bloom
  }

  def filterNew(df: DataFrame, col: String, bloom: BloomFilter[String]): DataFrame = {
    df.filter { row =>
      val v = row.getAs[String](col)
      if (v == null) true
      else if (!bloom.mightContain(v)) {
        bloom.put(v)
        true
      } else false
    }
  }
}
