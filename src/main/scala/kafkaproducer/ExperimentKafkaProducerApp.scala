package kafkaproducer
import model.Stock

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Success}

/**
  * Created by Satya on 11/12/2016.
  */

object ExperimentKafkaProducerApp extends App{

  val kafkaProducer = new ExperimentKafkaProducer("localhost","9092","experimentTopic")
  while(true)
    {
      val stock = new Stock("Apple",14.87566 + Random. nextInt(100))
      kafkaProducer.writeMessageToKafka(stock).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is apple and price is :" + stock.price)

        case Failure(ex) => println("Error in sending message....")
      }

      Thread.sleep(10000)
    }

 }
