SmartRoom – Streaming Classroom Recommendation System

SmartRoom is a real-time classroom recommendation and scheduling system built using Kafka, Spark Structured Streaming, and Big Data techniques.
The system suggests and reserves the most suitable university classroom based on course size, event type (lecture, exam, makeup, event), required facilities, and time availability.

🚀 Key Features

Real-time data ingestion using Kafka Producers

Streaming processing pipeline using Spark Structured Streaming

Intelligent room-matching based on capacity, time slots, and facilities

Support for different event types (lecture, exam, makeup class, event)

Kafka Consumer for real-time output visualization

Clean and scalable architecture following Big Data best practices

🏗️ Architecture
Kafka Producer → Kafka Topic (room_requests)
               ↓
        Spark Structured Streaming
               ↓
Kafka Topic (room_responses) → Consumer → Dashboard/Console

📂 Project Components

/producer – sends room booking events into Kafka

/spark-processing – processes the stream and applies matching logic

/consumer – reads matched rooms and displays results

/data – master data (rooms, schedules, course info)

💡 Technologies Used

Apache Kafka

Apache Spark Structured Streaming

Scala 

Big Data Pipelines

JSON Event Schemas

🎯 Purpose

Reduce scheduling conflicts and help faculty find the most suitable classroom instantly using real-time
