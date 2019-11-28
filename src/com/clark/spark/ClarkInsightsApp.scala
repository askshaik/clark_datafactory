package com.clark.spark

import java.util.logging.Logger
import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.TableFactory
import org.apache.spark.sql.{ SQLContext, SparkSession }

object ClarkInsightsApp {
  def main(args: Array[String]): Unit = {

    /*  ### Arguement List for ADLS Gen 1
    0 - Client ID for ADLS authentication
    1 - Credential for ADLS authentication
    2 - Refresh URL/Auth token for ADLS authentication
    3 - ADLS Account Name

    */

    val df = readJsonFile(spark)
    val flattenedDF = flattenDataFrame(spark, df)
    
    @transient lazy val log = Logger.getLogger(getClass.getName)
    try {
      val spark: SparkSession = SparkSession.builder().getOrCreate()
      spark.sparkContext.setLogLevel("WARN")

      spark.udf.register("regex_suffix", (input_name: String, input_clean_fix: String) => regex_suffix(input_name, input_clean_fix))
      log.info("SparkSession Created")

      /*    ADLS Gen 1 Configuration
      spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
      spark.conf.set("dfs.adls.oauth2.client.id", args(0).trim)
      spark.conf.set("dfs.adls.oauth2.credential", args(1).trim)
      spark.conf.set("dfs.adls.oauth2.refresh.url", s"https://login.microsoftonline.com/${args(2).trim}/oauth2/token")
*/

      //ADLS Gen 2 Configuration
      spark.conf.set(s"fs.azure.account.key.${args(0).trim}.dfs.core.windows.net", args(1).trim)
      spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

      spark.conf.set("spark.sql.crossJoin.enabled", "true")
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      log.info("Authenticating ADLS")

      val ssqc: SQLContext = spark.sqlContext

      log.info("Get Class from TableFactory for: " + args(2))
      println("Get Class from TableFactory for: " + args(2))
      TableFactory(args(2)).processRawToLowGrain(args, ssqc, log)

      //ADLS Gen 2 Configuration
      spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
    } catch {
      case unknown: Throwable => {
        println("Got this unknown exception: " + unknown)
        throw new Exception(unknown)
      }
    }

  }
}