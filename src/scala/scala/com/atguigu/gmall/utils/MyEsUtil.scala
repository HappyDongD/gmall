package scala.com.atguigu.gmall.utils

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

import scala.com.atguigu.gmall.bean.DauInfo

object MyEsUtil {

  var factory: JestClientFactory = null


  def getClient: JestClient = {
    if (factory == null) build();
    factory.getObject
  }

  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(5000).build())
  }

  def addDoc(): Unit = {
    val jest: JestClient = getClient

    val index = new Index.Builder(Movie0105("0104", "龙岭迷窟", "鬼吹灯")).index("movie0105_test_20200618").`type`("_doc").id("0103").build()
    jest.execute(index)
    print("我执行了")
    jest.close()
  }

  def main(args: Array[String]): Unit = {
    addDoc()
  }

  def queryDoc(): Unit = {
    val jest: JestClient = getClient

    //    new Search.Builder().addIndex("movie0105_test_20200618").build()
    //    jest.execute()
    jest.close()
  }

  def bulkDoc(sourceList: List[DauInfo], indexName: String): Unit = {
    if (sourceList != null || sourceList.size > 0) {
      val jest: JestClient = getClient
      var bulkBuilder = new Bulk.Builder() //构建批次操作
      for (source <- sourceList) {
        val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()

      val result: BulkResult = jest.execute(bulk)
      val items = result.getItems
      println("保存到ES:" + items.size() + "条数")
      jest.execute(bulk)
      jest.close()
    }
  }
  //
  //  def bulkDoc(sourceList: List[(String, Any)], indexName: String): Unit = {
  //    if (sourceList != null || sourceList.size > 0) {
  //      val jest: JestClient = getClient
  //      var bulkBuilder = new Bulk.Builder() //构建批次操作
  //      for (source <- sourceList) {
  //        val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
  //        bulkBuilder.addAction(index)
  //      }
  //      val bulk: Bulk = bulkBuilder.build()
  //
  //      val result: BulkResult = jest.execute(bulk)
  //      val items = result.getItems
  //      println("保存到ES:" + items.size() + "条数")
  //      jest.execute(bulk)
  //      jest.close()
  //    }
  //  }

  case class Movie0105(id: String, movie_name: String, name: String)
}
