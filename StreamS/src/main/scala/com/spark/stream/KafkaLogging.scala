package com.spark.stream

import java.util.Properties
import org.apache.log4j.{PropertyConfigurator, Logger}
import kafka.producer.KafkaLog4jAppender


object KafkaLogging {

  def main(args: Array[String]) {
    PropertyConfigurator.configure(getLog4jConfig)
    
  }
  private def getLog4jConfig: Properties = {
    var props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.Port", "9092")
    props.put("log4j.appender.KAFKA.Host", "localhost")
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")
    props
  }

}