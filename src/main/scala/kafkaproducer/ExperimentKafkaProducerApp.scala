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
      val stock1 = new Stock("Apple",14.87566 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock1).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is apple and price is :" + stock1.price)

        case Failure(ex) => println("Error in sending message....")
      }

      Thread.sleep(10000)

      val stock2 = new Stock("JP Morgan",5.5656 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock2).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is JP Morgan and price is :" + stock2.price)

        case Failure(ex) => println("Error in sending message....")
      }


      val stock3 = new Stock("Google",7.9090 + Random. nextInt(10))


      kafkaProducer.writeMessageToKafka(stock3).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is Google and price is :" + stock3.price)

        case Failure(ex) => println("Error in sending message....")
      }

      val stock4 = new Stock("Microsoft",6.7856 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock4).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is Microsoft and price is :" + stock4.price)

        case Failure(ex) => println("Error in sending message....")
      }


      val stock5 = new Stock("Deutsche Bank",2.8988 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock5).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is Deutsche bank and price is :" + stock5.price)

        case Failure(ex) => println("Error in sending message....")
      }


      val stock6 = new Stock("UBS",3.0004 + Random. nextInt(10))


      kafkaProducer.writeMessageToKafka(stock6).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is UBS and price is :" + stock6.price)

        case Failure(ex) => println("Error in sending message....")
      }

      val stock7 = new Stock("City Bank",2.04050 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock7).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is City Bank and price is :" + stock7.price)

        case Failure(ex) => println("Error in sending message....")
      }


      val stock8 = new Stock("Goldman Sachs",1.099 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock8).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is Goldman Sache and price is :" + stock8.price)

        case Failure(ex) => println("Error in sending message....")
      }

      val stock9 = new Stock("GAM",8.0999 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock9).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is GAM and price is :" + stock9.price)

        case Failure(ex) => println("Error in sending message....")
      }


      val stock = new Stock("Marshalways",0.9122 + Random. nextInt(10))

      kafkaProducer.writeMessageToKafka(stock).onComplete {
        case Success(ground) => println("Sent messages successfully....The stock is Marshalways and price is :" + stock.price)

        case Failure(ex) => println("Error in sending message....")
      }


      Thread.sleep(100)
    }

 }
