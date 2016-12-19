package kafkaconsumer
import Serialization.StockByteArraySerializer
import model.Stock
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Satya on 11/12/2016.
  */
object KafkaStreamConsumer extends App{

  startKafkaStreaming()

  def startKafkaStreaming()={
      val conf = new SparkConf().setMaster("local[2]").setAppName("Average Price Calculator")
      val sparkStreamingContext = new StreamingContext(conf, Seconds(10))
     sparkStreamingContext.checkpoint("F:\\BigData\\Kafka")
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[ByteArrayDeserializer ],
        "group.id" -> "Apple_Stock_Group_ID",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val topics = Array("experimentTopic")
      val stockStream = KafkaUtils.createDirectStream[String, Array[Byte]](
        sparkStreamingContext,
        PreferConsistent,
        Subscribe[String, Array[Byte]](topics, kafkaParams)
      )



    val createPriceCombiner = (price: BigDecimal) => (1, price)

    val priceCombinor = (collector: (BigDecimal,Int), price: BigDecimal) => {
      val (numberScores, totalScore) = collector
      (numberScores + 1, totalScore + price)
    }

    val scoreMerger = (collector1: (BigDecimal,Int), collector2: (BigDecimal,Int)) => {
      val (numScores1, totalScore1) = collector1
      val (numScores2, totalScore2) = collector2
      (numScores1 + numScores2, totalScore1 + totalScore2)
    }


      val scores = stockStream
        .map(x=>
        {
          val stock = StockByteArraySerializer.deserialize(x.value()).asInstanceOf[Stock]
          new Tuple2(stock.stockName,stock.price)
        })
        .combineByKey(createPriceCombiner, priceCombinor,scoreMerger,new HashPartitioner(3)).foreachRDD(x=>println(x.collect()))


        //  .updateStateByKey(updateSumOfPrices).

      sparkStreamingContext.start()
      sparkStreamingContext.awaitTermination()
    }

  def updateSumOfPrices(previousPrices: Seq[BigDecimal], newlyArrivedPrice: Option[BigDecimal]): Option[BigDecimal] = {
    val sumOfPrevPrices = previousPrices.sum
    val stockCount = previousPrices.size +1;
    val newPrice = newlyArrivedPrice.getOrElse[BigDecimal](0.0d)
    Some((sumOfPrevPrices + newPrice)/stockCount)
  }

}
