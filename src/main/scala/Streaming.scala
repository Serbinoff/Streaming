import java.util.Properties
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import scalaj.http.{Http, HttpResponse}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Success}

object Streaming extends App {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val sparkConf = new SparkConf().setAppName("StreamingJob").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "earliest"
    )

    val topics = Set("oldKafkaTopic")
    val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    messages.foreachRDD(rdd => rdd.map(record => Utils.fromJson[KafkaRequest](record.value())).foreach(i =>
      fetchInfo(CurrencyRequest(i.price, "USD", args(0)))
        .onComplete {
          case Success(res) => producer.send(new ProducerRecord("newKafkaTopic", "key", Utils.toJson(KafkaResponse(i.id, res.body.toDouble, args(0)))))
          case Failure(e) => println(s"Data fetch failed: ${e.getStackTrace}")
        })
    )

    ssc.start()
    ssc.awaitTermination()

    def fetchInfo(currencyRequest: CurrencyRequest): Future[HttpResponse[String]] = {
      val message = Http("http://localhost:8080/convertor")
        .postData(
          s"""{"value": ${currencyRequest.value}, "fromCurrency": "${currencyRequest.fromCurrency}", "toCurrency": "${currencyRequest.toCurrency}"}""")
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(scalaj.http.HttpOptions.readTimeout(10000)).asString
      Future {
        blocking {
          message
        }
      }
    }
}

case class KafkaRequest(id: Int, price: Int)
case class KafkaResponse(id: Int, price: Double, currency: String)
case class CurrencyRequest(value: Double, fromCurrency: String, toCurrency: String)
case class CurrencyResponse(price: Int, currency: String)
