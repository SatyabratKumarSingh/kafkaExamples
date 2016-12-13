package kafkaproducer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties

import Serialization.StockByteArraySerializer
import model.Stock

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

/**
  * Created by Satya on 11/12/2016.
  */
class ExperimentKafkaProducer(hostName:String, port : String, topicName:String) {

  val kafkaConfig = createKafkaConfig()


  def createKafkaConfig(): Properties =
    {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostName + ":" + port)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      return props
    }

   def writeMessageToKafka(message:Stock)= Future{
     val record = new ProducerRecord[String, Array[Byte]](topicName, null, StockByteArraySerializer.serialize(message))
     val producer = new KafkaProducer[String, Array[Byte]](kafkaConfig)
     producer.send(record);
     producer.close()
     producer.flush()
   }
}
