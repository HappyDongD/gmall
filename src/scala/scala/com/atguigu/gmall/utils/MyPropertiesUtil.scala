package scala.com.atguigu.gmall.utils



import java.io.{FileInputStream, InputStreamReader}
import java.util.Properties

/**
 * @Description
 * @author zhang dong
 * @date 2021/5/17-11:29
 */
object MyPropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
