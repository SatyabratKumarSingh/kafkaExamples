package kafkaproducer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * Created by Satya on 11/12/2016.
  */

object ExperimentKafkaProducerApp extends App{

  val kafkaProducer = new ExperimentKafkaProducer("localhost","9092","experimentTopic")

  var  i = 0 ;
  while(i <10){
    println("Sending message to kafka")
    kafkaProducer.writeMessageToKafka("Satya " + i ).onComplete {
      case Success(ground) => println("Sent messages successfully....")

      case Failure(ex) => println("Error in sending message....")
    }

    Thread.sleep(1000);
     i+=1
  }
}
