package scala.com.atguigu.gmall.dim

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.Seconds

import scala.com.atguigu.gmall.utils.{MyKafkaUtil, OffsetManagerUtil}

/**
 * @Description
 * @author zhang dong
 * @date 2021/5/26-14:53
 */
object   ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("province")
    val ssc = new StreamingContext(conf, Seconds(2))

    var topic="ods_base_province"
    var groupId="province_group"
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


  }
}
