package scala.com.atguigu.gmall.ods

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.com.atguigu.gmall.utils.{MyKafkaUtil, OffsetManagerUtil}


/**
 * @Description
 * @author zhang dong
 * @date 2021/5/24-9:40
 */
object BaseDbmaxwellApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("maxwellApp")
      .setMaster("localhost[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    var topic = "gmall_db_m"
    var groupId = "maxwell"
    //获取偏移量
    val offset: Map[TopicPartition, Long] = OffsetManagerUtil
      .getOffset(topic, groupId)
    var value: InputDStream[ConsumerRecord[String, String]] = null
    if (offset != null && offset.nonEmpty) {
      value = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      value = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //取出偏移量信息
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val RecordDstream: DStream[ConsumerRecord[String, String]] = value.transform((rdd: RDD[ConsumerRecord[String, String]]) => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

  }

}
