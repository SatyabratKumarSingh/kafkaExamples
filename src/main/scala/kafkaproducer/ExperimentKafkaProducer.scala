package kafkaproducer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties

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
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      return props
    }

   def writeMessageToKafka(message:String)= Future{
     val record = new ProducerRecord[String, String](topicName, null, message)
     val producer = new KafkaProducer[String, String](kafkaConfig)
     producer.send(record);
     Thread.sleep(Random.nextInt(500))

   }

  def closeProducer()={
   // producer.close()
  }
}
