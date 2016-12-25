package kafkaconsumer

import model.{Stock, StockCountry}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.JoinType

/**
  * Created by Satya on 24/12/2016.
  */


/* An Example to read a CSV file and put that into a data set */

object DataSetsOperations extends App{

  val sparkSession = SparkSession.builder.
    master("local[5]")
    .appName("Spark Data Sets Example")
    .getOrCreate()

  import sparkSession.implicits._

    val stocksDs = sparkSession.read
      .option("header","true")
      .csv("F:\\Scala\\example\\kafkaExamples\\Stocks.csv")
        .map(row=> Stock(row.get(0).asInstanceOf[String],
          BigDecimal(row.get(1).asInstanceOf[String])))


  val anotherStockDs = sparkSession.read
    .option("header","true")
    .csv("F:\\Scala\\example\\kafkaExamples\\AnotherStocks.csv")
    .map(row=> Stock(row.get(0).asInstanceOf[String],
      BigDecimal(row.get(1).asInstanceOf[String])))

   val unionOfStocks = stocksDs.union(anotherStockDs)


  val stockCountries = sparkSession.read
    .option("header","true")
    .csv("F:\\Scala\\example\\kafkaExamples\\AnotherStocks.csv")
    .map(row=> StockCountry(row.get(0).asInstanceOf[String],
      row.get(1).asInstanceOf[String]))


   val stockWithCountries = unionOfStocks.
     join(stockCountries,unionOfStocks("stockName") === stockCountries("stockName"))

    
  stockWithCountries.show()

}
