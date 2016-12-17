package kafkaconsumer
import Serialization.StockByteArraySerializer
import model.Stock
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
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

      stockStream
        .map(x=>
        {
          val stock = StockByteArraySerializer.deserialize(x.value()).asInstanceOf[Stock]
          new Tuple2(stock.stockName,stock.price)
        }).reduceByKey(_ + _).updateStateByKey(updateSumOfPrices)
          .foreachRDD(x=>x.collect().foreach(println))

      sparkStreamingContext.start()
      sparkStreamingContext.awaitTermination()
    }


  def updateSumOfPrices(previousPrices: Seq[BigDecimal], newlyArrivedPrice: Option[BigDecimal]): Option[BigDecimal] = {
    val sumOfPrevPrices = previousPrices.sum
    val newPrice = newlyArrivedPrice.getOrElse[BigDecimal](0.0d)
    Some(sumOfPrevPrices + newPrice)
  }

}
