package kafkaconsumer
import Serialization.StockByteArraySerializer
import model.Stock
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Satya on 11/12/2016.
  */
object KafkaStreamAverageCalculation extends App{

  startKafkaStreaming()

  def startKafkaStreaming()={
    val conf = new SparkConf().setMaster("local").setAppName("Average Price Calculator")
      .set("spark.driver.allowMultipleContexts","true")
    val sparkContext = new SparkContext(conf)
    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(10))

    // Mandatory to provide checkpoint if you are using update by key.
    sparkStreamingContext.checkpoint("F:\\BigData\\Kafka")

    val stockStaticData = sparkContext.textFile("F:\\Scala\\example\\kafkaExamples\\StockPrices.txt")
      .map(_.split(','))
      .map(line => Tuple2(line(0),BigDecimal(line(1))))


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

    val stockDStream = stockStream
      .map(x=>
      {
        val stock = StockByteArraySerializer.deserialize(x.value()).asInstanceOf[Stock]
        new Tuple2(stock.stockName,stock.price)
      })

    // Join with the static RDD.
    val combinedStream = stockDStream.transform(x=>x.union(stockStaticData))


    // Dynamic Average calculation from DStream
    combinedStream.reduceByKey(_ + _).updateStateByKey(updateSumOfPrices)
      .foreachRDD(x=>x.collect().foreach(println))

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }


  def updateSumOfPrices(previousPrices: Seq[BigDecimal], newlyArrivedPrice: Option[BigDecimal]): Option[BigDecimal] = {
    val sumOfPrevPrices = previousPrices.sum
    val previousSize = previousPrices.size;
    val currentSize = previousSize + 1
    val newPrice = newlyArrivedPrice.getOrElse[BigDecimal](0.0d)
    if(newPrice > 0)
    Some((sumOfPrevPrices + newPrice)/currentSize)
    else
      Some(sumOfPrevPrices)
  }

}
