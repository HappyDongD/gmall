package scala.com.atguigu.gmall.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.com.atguigu.gmall.bean.DauInfo
import scala.com.atguigu.gmall.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}

/**
 * @Description
 * @author zhang dong
 * @date 2021/5/18-10:24
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("DauApp")
    val context = new
        StreamingContext(conf, Seconds(5))
    val topic: String = "GMALL_EVENT_0105"
    val groupId: String = "GMALL_DAUP"
    val kafkaInputD: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(topic, context, groupId)
    print("消费数据了")
    val value: DStream[JSONObject] = kafkaInputD
      .map((e: ConsumerRecord[String, String]) => {
        val str: String = e.value()
        val json: JSONObject = JSON.parseObject(str)
        val ts: lang.Long = json
          .getLong("ts")
        val tsDate: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))

        val strings: Array[String] = tsDate
          .split(" ")
        val dt = strings(0);
        val ht = strings(1)
        json.put("hr", ht)
        json.put("dt", dt)
        json
      })
    //数据去重
    val filterD: DStream[JSONObject] = value.mapPartitions(e => {
      val jedis: Jedis = RedisUtil.getJedisClient
      val filterList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
      for (elem <- e) {
        val dt: String = elem
          .getString("dt")
        val mid: String = elem
          .getJSONObject("common")
          .getString("mid")
        val key = "dau:" + dt
        val long: lang.Long = jedis
          .sadd(key, mid)
        if (jedis.ttl(key) < 0) {
          jedis.expire(key, 3600 * 24)
        }
        if (long == 1L) {
          filterList.append(elem)
        }
      }
      filterList.toIterator
    })
    //存入es当中
    filterD.foreachRDD(e => {
      e.foreachPartition((f: Iterator[JSONObject]) => {
        val list: List[DauInfo] = f.map { startupJsonObj =>
          val commonJSONObj: JSONObject = startupJsonObj.getJSONObject("common")
          val dauInfo: DauInfo = DauInfo(commonJSONObj.getString("mid"), commonJSONObj.getString("uid"), commonJSONObj.getString("mid"), commonJSONObj.getString("ch")
            , commonJSONObj.getString("vc"), commonJSONObj.getString("dt"), commonJSONObj.getString("hr"), "00", startupJsonObj.getLong("ts"))
          dauInfo
        }.toList
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        MyEsUtil.bulkDoc(list, "gmall_dau_info" + dateStr)
      })})
      context.start()
      context.awaitTermination()

    }
  }
