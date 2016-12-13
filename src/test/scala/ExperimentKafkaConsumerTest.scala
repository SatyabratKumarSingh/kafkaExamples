import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafkaproducer.ExperimentKafkaProducer
import collection.mutable.Stack
import org.scalatest._


/**
  * Created by Satya on 11/12/2016.
  */
class ExperimentKafkaConsumerTest extends FlatSpec with Matchers {

  it should "set the properties correctly " in {
    val producer = new ExperimentKafkaProducer("localhost","9092","testTopic")
    val config = producer.createKafkaConfig()
    println(config.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString)
     // "localhost:9092" == "localhost:9092" should (true)
  }
}
