package spark


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 通过DirectAPI 消费Kafka数据
 * 消费的offset保证在_consumer_offset主题中
 */
object SparkStream01_DirectAPI010 {
  def main(args: Array[String]): Unit = {
        //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStream01_DirectAPI010")

    //创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //定义kafka相关的连接参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //通过读取kafka数据，创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策略，指定计算的Excutor
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](Set("bigdata0105"), kafkaParams)
    )

    //wordcount计算
    kafkaDStream.map(_.value())
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    //启动采集任务
    ssc.start()
    ssc.awaitTermination()
    
     

  }
    
  
}
