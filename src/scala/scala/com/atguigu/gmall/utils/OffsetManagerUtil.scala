package scala.com.atguigu.gmall.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ListBuffer

/**
 * @Description
 * @author zhang dong
 * @date 2021/5/19-17:12
 */
object OffsetManagerUtil {
  //  get from redis , type:hash  key:offset:topic:groupId  field:partition  value: offset
  def getOffset(topic: String, groupId: String) = {
    val client: Jedis = RedisUtil.getJedisClient
    //get key
    val key = "offset:" + topic + ":" + groupId
    val offsetValue: util.Map[String, String] = client.hgetAll(key)
    client.close()
    import scala.collection.JavaConverters._
    val map: Map[TopicPartition, Long] = offsetValue
      .asScala.map {
      case (partition, offset) => {
        (new TopicPartition(topic, partition.toInt)
          , offset.toLong)
      }
    }.toMap
    map
  }
  def saveOffsets(offsetRangers: Array[OffsetRange], groupId: String, topic: String): Unit = {
    import java.util

    import redis.clients.jedis.Jedis
    val client: Jedis = RedisUtil.getJedisClient
    val key = s"offset:${groupId}:${topic}"
    val fieldAndValue: util.Map[String, String] = offsetRangers.map(offsetRanger => {
      //将offsetRanger中的分区和offset构成一个元组返回
      offsetRanger.partition.toString -> offsetRanger.untilOffset.toString
    })
      //元组可以直接toMap 转成map
      .toMap
      .asJava
    //    String hmset(key: String, hash: util.Map[String, String])
    //为什么不用hset   由于一个key对应多个元组
    client.hmset(key, fieldAndValue)
    println("保存偏移量 topic_partition->offset" + fieldAndValue)
    client.close()
  }

}
