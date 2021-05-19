package scala.com.atguigu.gmall.utils

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

import java.util

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
}
