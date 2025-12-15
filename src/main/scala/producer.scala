package producer

import java.io.File
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object producer {

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
      val parser = getParser(file.getName)

      if (parser == null) {
        println(s"⚠️ No parser found for file: ${file.getName}")
      } else {
        for (line <- lines) {
          if (line.trim.nonEmpty) {
            val json = parser(line)
            if (json != null) {
              val record = new ProducerRecord[String, String](topic, json)
              producer.send(record)
              counter += 1
              println(s"✅ Sent event #$counter: $json")

              Thread.sleep(200)   // ✅ سرعة مناسبة للـ streaming
            }
          }
        }
      }
    }

    producer.close()
    println(s"\n✅✅ Done! Sent a total of $counter events to Kafka.")
    println("✅ Stream completed successfully.")
  }

  // اختيار الدالة المناسبة حسب اسم الملف
  def getParser(filename: String): String => String = {
    val name = filename.toLowerCase
    if (name.contains("bookings") && !name.contains("onetime")) csvToJsonFixed
    else if (name.contains("onetime")) csvToJsonOneTime
    else if (name.contains("courses")) csvToJsonCourses
    else if (name.contains("sections")) csvToJsonSections
    else if (name.contains("professors")) csvToJsonProfessors
    else if (name.contains("classroom")) csvToJsonClassroom
    else null
  }

  // دالة مشتركة للوقت
  def timestamps(): (String, String) = {
    val now = LocalDateTime.now(ZoneId.of("Asia/Jerusalem"))
    val ts = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val date = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    (ts, date)
  }

  // ✅ جدول البوكينغ الثابت
  def csvToJsonFixed(line: String): String = {
    val parts = line.split(";")
    if (parts.length < 7) return null
    val (ts, date) = timestamps()

    s"""{
       "source_type": "fixed_booking",
       "booking_id": "${parts(0).trim}",
       "section_id": "${parts(1).trim}",
       "classroom_id": "${parts(2).trim}",
       "date": "${parts(3).trim}",
       "start_time": "${parts(4).trim}",
       "end_time": "${parts(5).trim}",
       "students": ${parts(6).trim},
       "ingestion_timestamp": "$ts",
       "ingestion_date": "$date"
    }"""
  }

  // ✅ جدول الحجز لمرة واحدة
  def csvToJsonOneTime(line: String): String = {
    val parts = line.split(";")
    if (parts.length < 8) return null
    val (ts, date) = timestamps()

    s"""{
       "source_type": "one_time_booking",
       "onetime_id": "${parts(0).trim}",
       "professor_id": "${parts(1).trim}",
       "classroom_id": "${parts(2).trim}",
       "date": "${parts(3).trim}",
       "start_time": "${parts(4).trim}",
       "end_time": "${parts(5).trim}",
       "students": ${parts(6).trim},
       "booking_type": "${parts(7).trim}",
       "ingestion_timestamp": "$ts",
       "ingestion_date": "$date"
    }"""
  }

  // ✅ جدول القاعات
  def csvToJsonClassroom(line: String): String = {
    val parts = line.split(";")
    if (parts.length < 4) return null
    val (ts, date) = timestamps()

    s"""{
       "source_type": "classroom",
       "classroom_id": "${parts(0).trim}",
       "college_id": "${parts(1).trim}",
       "room_number": "${parts(2).trim}",
       "capacity": ${parts(3).trim},
       "ingestion_timestamp": "$ts",
       "ingestion_date": "$date"
    }"""
  }

  // ✅ جدول الكورسات
  def csvToJsonCourses(line: String): String = {
    val parts = line.split(";")
    if (parts.length < 4) return null
    val (ts, date) = timestamps()

    s"""{
       "source_type": "courses",
       "course_id": "${parts(0).trim}",
       "course_name": "${parts(1).trim}",
       "department": "${parts(2).trim}",
       "fixed_students": ${parts(3).trim},
       "ingestion_timestamp": "$ts",
       "ingestion_date": "$date"
    }"""
  }

  // ✅ جدول الدكاترة
  def csvToJsonProfessors(line: String): String = {
    val parts = line.split(";")
    if (parts.length < 4) return null
    val (ts, date) = timestamps()

    s"""{
       "source_type": "professors",
       "professor_id": "${parts(0).trim}",
       "name": "${parts(1).trim}",
       "department": "${parts(2).trim}",
       "college_id": "${parts(3).trim}",
       "ingestion_timestamp": "$ts",
       "ingestion_date": "$date"
    }"""
  }

  // ✅ جدول السكاشن
  def csvToJsonSections(line: String): String = {
    val parts = line.split(";")
    if (parts.length < 8) return null
    val (ts, date) = timestamps()

    s"""{
       "source_type": "sections",
       "section_id": "${parts(0).trim}",
       "course_id": "${parts(1).trim}",
       "professor_id": "${parts(2).trim}",
       "day_schedule": "${parts(3).trim}",
       "start_hour": "${parts(4).trim}",
       "duration_hours": "${parts(5).trim}",
       "classroom_id": "${parts(6).trim}",
       "fixed_students": ${parts(7).trim},
       "ingestion_timestamp": "$ts",
       "ingestion_date": "$date"
    }"""
  }
}
