package kafkaconsumer
import Serialization.StockByteArraySerializer
import model.Stock
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
/**
  * Created by Satya on 11/12/2016.
  */
object KafkaStreamConsumer extends App{

  startKafkaStreaming()

  def startKafkaStreaming()={
      val conf = new SparkConf().setMaster("local[2]").setAppName("Average Price Calculator")
      val sparkStreamingContext = new StreamingContext(conf, Seconds(10))

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
        }).reduceByKey(_ + _).updateStateByKey(updateFunc _,new HashPartitioner(10), false)
          .foreachRDD(x=>x.collect().foreach(println))



      sparkStreamingContext.start()
      sparkStreamingContext.awaitTermination()
    }


  def updateStateForKey(values: Seq[BigDecimal], state: Option[BigDecimal]): Option[BigDecimal] = {
    val currentCount = values.sum
    val previousCount = state.getOrElse[BigDecimal](0.0d)
    Some(currentCount + previousCount)
  }

  // The custom update function that takes in an iterator of key, new values for the key, previous state for the key and returns a new iterator of key, value
  def updateFunc(iter: Iterator[(String, Seq[BigDecimal], Option[BigDecimal])]): Iterator[(String, BigDecimal)] = {
    val list = ListBuffer[(String, BigDecimal)]()
    while(iter.hasNext) {
      val record = iter.next
      val state = updateStateForKey(record._2, record._3)
      val value = state.getOrElse[BigDecimal](0.0d)
      if( value != 0 ) {
        list += ((record._1, value))              // Add only keys with positive counts
      }
    }
    list.toIterator
  }

  def updateFunction(newAverages : Seq[BigDecimal], currentAvg: Option[BigDecimal]): BigDecimal = {
    var total = currentAvg.getOrElse[BigDecimal](0.0d)
    newAverages.foreach((price: BigDecimal) => total = total + price)
    return total
  }
}
