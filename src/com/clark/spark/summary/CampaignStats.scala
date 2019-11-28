package com.clark.spark.summary
import java.util.logging.Logger

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import org.apache.spark.sql.{ SQLContext, SaveMode }
class CampaignStats extends Table {

  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Summary Stats for table : " + "$tableName")
    val adlsPath = getAdlsPath(adlsName)

        ssqc.read.parquet(adlsPath + s"email_campaign/$client/$brand/lg/email_campaign_response_fact/$deltaFolder").createOrReplaceTempView("email_campaign_response_fact")

        ssqc.sql(s"select $brandId as brand_id, '$loadType' as load_type, cast('$rawCopyTime' as integer) as raw_copy_time, cast('$sparkProcessingTime' as integer) as spark_processing_time, cast('$sqlCopyTime' as integer) as sql_copy_time, '$adlsLgPath' as adls_lg_path, '$sqlLgTableName' as sql_table_name, nvl(ce.number_of_campaigns_transferred,0) as number_of_campaigns_transferred, ce.date_first_campaign_sent_with_campaign_name as date_first_campaign_sent_with_campaign_name , ce.date_last_campaign_sent_with_campaign_name as date_last_campaign_sent_with_campaign_name ,ee.campaign_name as campaign_name  , ce.campaign_id as campaign_id ,nvl(c.open_cnt_avg_weekly,0) as total_number_of_open_per_week , a.first_date_of_open as first_date_of_open , a.last_date_of_open as last_date_of_open ,nvl(d.clk_cnt_avg_weekly,0) as  total_number_of_click_per_week ,a.first_date_of_click  as first_date_of_click ,a.last_date_of_click as last_date_of_click,nvl(a.total_number_of_unsubscribe,0)  as  total_number_of_unsubscribe, a.first_date_of_unsubscribe as first_date_of_unsubscribe, a.last_date_of_unsubscribe  as last_date_of_unsubscribe,  current_timestamp as etl_created_date, current_timestamp as etl_updated_date, 'fbiadmin' as etl_created_by, 'fbiadmin' as etl_updated_by, $batchId as etl_batch_id, 'Email Campaign' as etl_source from email_campaign_exec_temp ce join email_campaign_dim  ee on ce.campaign_id=ee.campaign_id  left outer join email_campaign_response_temp a on ce.campaign_id=a.campaign_id left outer join  email_open_cnt_avg_weekly c on ce.campaign_id= c.campaign_id left outer join email_click_cnt_avg_weekly d on ce.campaign_id= d.campaign_id ").write.mode(SaveMode.Overwrite).parquet(adlsPath + s"email_campaign/$client/$brand/summary/email_campaign_historical/$deltaFolder")

  }
}