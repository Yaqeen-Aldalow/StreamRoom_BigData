package producer

import java.io.File
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object CsvBatchProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "room_requests"

    val folder = new File("data")
    if (!folder.exists() || !folder.isDirectory) {
      println("❌ Folder 'data' not found. Create it and add CSV files.")
      return
    }

    val files = folder.listFiles().filter(_.getName.endsWith(".csv"))
    println(s"✅ Found ${files.length} CSV files")

    var counter = 0

    for (file <- files) {
      println(s"\n📌 Processing file: ${file.getName}")

      val lines = Source.fromFile(file).getLines().drop(1)

      for (line <- lines) {

        if (line.trim.isEmpty) {
          // skip empty lines silently
        } else {

          val json = csvToJson(line)

          if (json != null) {
            val record = new ProducerRecord[String, String](topic, json)
            producer.send(record)

            counter += 1

            // ✅ اطبعي كل Event
            println(s"✅ Sent event #$counter: $json")

            // ✅ كل 5 ثواني
            Thread.sleep(5000)
          }
        }
      }
    }

    producer.close()

    println(s"\n✅✅ Done! Sent a total of $counter events to Kafka.")
    println("✅ Stream completed successfully.")
  }

  def csvToJson(line: String): String = {
    val parts = line.split(";")

    if (parts.length < 8) {
      return null
    }

    try {
      val now = LocalDateTime.now(ZoneId.of("Asia/Jerusalem"))
      val timestamp = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      val dateOnly = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

      s"""{
         "course_id": "${parts(0).trim}",
         "department": "${parts(1).trim}",
         "students_count": ${parts(2).trim},
         "days": "${parts(3).trim}",
         "time": "${parts(4).trim}",
         "type": "${parts(5).trim}",
         "building": "${parts(6).trim}",
         "room": "${parts(7).trim}",
         "ingestion_timestamp": "$timestamp",
         "ingestion_date": "$dateOnly"
      }"""
    } catch {
      case _: Exception => null
    }
  }
}
