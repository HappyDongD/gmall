package scala.com.atguigu.gmall.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.com.atguigu.gmall.utils.{MyKafkaUtil, OffsetManagerUtil}

/**
 * @Description
 * @author zhang dong
 * @date 2021/5/21-15:05
 */
object BaseDBcanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("odsCanal").setMaster("local[4]")
    val ssc = new
        StreamingContext(conf, Seconds(5))
    val topic = "gmall0523_db_c"
    val groupId = "base_db_canal_group"
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    //从redis中获取偏移量
    var partitionToLong: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (partitionToLong != null && partitionToLong.nonEmpty) {
      //从指定偏移量开始消费
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, partitionToLong, groupId)
    } else {
      //从最新位置
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //获取当前批次读取的kafka主题中的偏移量信息
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform((rdd: RDD[ConsumerRecord[String, String]]) => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //对接收到的数据进行结构转换
    val jsonDstream: DStream[JSONObject] = offsetDstream.map(record => {
      val jsonStr: String = record.value()
      //转为JSON对象
      val jsonObject: JSONObject = JSON.parseObject(jsonStr)
      jsonObject
    })
    //分流处理，根据不同的表名发送到不同的kafka主题
    jsonDstream.foreachRDD(rdd => {
      rdd.foreach(json => {
        //获取操作类型
        val optType: String = json.getString("type")
        if ("INSERT".equals(optType)) {
          //获取表名
          val tableName: String = json.getString("table")
          //获取数据
          val jsonArray: JSONArray = json.getJSONArray("data")
          //拼接目标topic
          var target = "ods_" + tableName
          import scala.collection.JavaConverters._
          //对data进行遍历
          for (elem <- jsonArray.asScala) {
            MyKafkaUtil.send(target, elem.toString)
          }
        }
      })
    })
    //保存偏移量
    OffsetManagerUtil.saveOffsets(ranges,groupId, topic)

  }

}
