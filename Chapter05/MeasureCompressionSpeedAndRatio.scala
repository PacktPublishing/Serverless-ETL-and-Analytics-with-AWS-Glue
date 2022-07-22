import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection._
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.s3.model.{ListObjectsV2Request, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}


object GlueJobUtils {
    private var sparkJobSecondDuration = 0.toDouble
    def setJobDuration(milliDuration: Long): Unit = {
        this.sparkJobSecondDuration = milliDuration / 1000.toDouble
    }

    def getJobDuration: Double = this.sparkJobSecondDuration
    def getLogger = new GlueLogger
    def epochToString(epochMillis: Long): String = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(epochMillis)
}


class JobEventManager extends SparkListener {
    private val eventConsumerMap = mutable.Map[String, JobEventConsumer]()

    def addEventConsumer(sc: SparkContext, id: String, jobEventConsumer: JobEventConsumer) = eventConsumerMap += (id -> jobEventConsumer)

    def removeEventConsumer(id: String) = eventConsumerMap -= id

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        eventConsumerMap.foreach{ case (_, jec) =>
            if(jec != null) jec.onJobStart(jobStart)
        }
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        eventConsumerMap.foreach{ case (_, jec) =>
            if(jec != null ) jec.onJobEnd(jobEnd)
        }
    }
}


trait JobEventConsumer {
    def onJobStart(jobStart: SparkListenerJobStart)
    def onJobEnd(jobEnd: SparkListenerJobEnd)
}

class JobEventConsumerImpl extends JobEventConsumer {
    private var startTime: Long = 0
    private val logger = GlueJobUtils.getLogger

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        this.startTime = jobStart.time
        this.logger.info(s"Job-${jobStart.jobId} started.\njob Start time: ${GlueJobUtils.epochToString(this.startTime)}")
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        val milliDuration: Long = jobEnd.time - this.startTime
        GlueJobUtils.setJobDuration(milliDuration)
        this.logger.info(s"Job-${jobEnd.jobId} finished.\njob End time: ${GlueJobUtils.epochToString(jobEnd.time)} \njob duration (sec): ${GlueJobUtils.getJobDuration}.")
    }
}


class GlueMeasurement(spark: SparkSession, dstBucket: String, dstPath: String) {
    private val logger = GlueJobUtils.getLogger
    private val dstS3BasePath = s"s3://${dstBucket}/${dstPath}"

    def generateTpcdsDataset(tableList: List[String]): Unit = {
        for(table <- tableList) {
        this.logger.info(s"Generating TPC-DS ${table} table dataset.")
        val df = spark.read.format("tpcds")
            .option("table", table)
            .option("scale", 1000)
            .option("numPartitions", 36)
            .load()
        df.write.option("compression", "none")
            .parquet(s"${dstS3BasePath}/compression=none/table=$table")
        this.logger.info(s"Completed generating the $table dataset.")

        }
    }

    def measureCompressionDecompressionSpeed(tableList: List[String], compressionCodecList: List[String]): Unit = {
        def measureCompressionDecompressionSpeedForEachTable(table: String, compressionCodecList: List[String]): Unit = {
        this.logger.info(s"Retrieving ${table} no compressed data...")
        val df = spark.read.parquet(s"${dstS3BasePath}/compression=none/table=${table}")
        for(compressionCodec <- compressionCodecList) {
                val s3path = s"${dstS3BasePath}/compression=${compressionCodec}/table=${table}"
                this.logger.info(s"Writing ${table} data with $compressionCodec compression")
                df.write.option("compression", compressionCodec).parquet(s3path)
                this.logger.info(s"Done. Writing the ${table} data with ${compressionCodec} compression Spark Job Duration (sec): ${GlueJobUtils.getJobDuration}")

                this.logger.info(s"Measuring decompression speed for ${compressionCodec}")
                spark.read.parquet(s3path)
                this.logger.info(s"Done. Reading the ${table} data with ${compressionCodec} decompression Spark Job Duration (sec): ${GlueJobUtils.getJobDuration}")
            }
        }

        for(table <- tableList) measureCompressionDecompressionSpeedForEachTable(table, compressionCodecList)
    }

    def measureCompressionRatio(tableList: List[String], compressionCodecList: List[String], baseCodec: String): Unit = {
        def getApproxDataSize(s3c: AmazonS3, compressionCodec: String): Long = {
            val listObjectsV2Request = new ListObjectsV2Request()
                                                .withBucketName(dstBucket)
                                                .withPrefix(s"$dstPath/codec=$compressionCodec/")
            val listObjectsV2Result = s3c.listObjectsV2(listObjectsV2Request)
            val objectSummaries: List[S3ObjectSummary] = listObjectsV2Result.getObjectSummaries.asScala.toList
            var dataSize = objectSummaries.map(objSummary => objSummary.getSize).sum

            while(listObjectsV2Result.isTruncated) {
                listObjectsV2Request.setContinuationToken(listObjectsV2Result.getNextContinuationToken)

                val listObjectsV2ResultNext = s3c.listObjectsV2(listObjectsV2Request)
                dataSize += s3c.listObjectsV2(listObjectsV2Request).getObjectSummaries.asScala.toList.map(objSummary => objSummary.getSize).sum

                listObjectsV2Result.setContinuationToken(listObjectsV2ResultNext.getContinuationToken)
                listObjectsV2Result.setNextContinuationToken(listObjectsV2ResultNext.getNextContinuationToken)
                listObjectsV2Result.setTruncated(listObjectsV2ResultNext.isTruncated)
            }
            dataSize
        }

        val s3c = AmazonS3ClientBuilder.standard().build()
        compressionCodecList.foreach(compressionCodec => {
            val baseCodecDataSize = getApproxDataSize(s3c, baseCodec)
            if(compressionCodec.equals(baseCodec))
                this.logger.info(s"compression ratio (parquet.$compressionCodec/parquet.$baseCodec) = 1.00")
            else {
                val codecDataSize = getApproxDataSize(s3c, compressionCodec)
                this.logger.info(s"compression ratio (parquet.$compressionCodec/parquet.$baseCodec) " +
                                                    s"= ${codecDataSize.toDouble/baseCodecDataSize.toDouble}")
            }
        })
    }
}


/**
 * Main part of this measurement.
 * Before running this script, set your bucket name and path where you want to put the TPC-DS data.
 * The measurement process runs as the following steps:
 * 1. Generate TPC-DS dataset and write them as Parquet files without compression in the specified S3 location
 * 2. Measure compression and decompression speed by reading and writing the data
 * 3. Measure compression ratio between the Parquet files with compression and without compression
 */
object GlueApp {
    def main(sysArgs: Array[String]) {
        val sc: SparkContext = new SparkContext()
        val glueContext: GlueContext = new GlueContext(sc)
        val spark = glueContext.getSparkSession

        // Initilize Job Listener
        val jem = new JobEventManager
        jem.addEventConsumer(sc, "JobEventConsumer", new JobEventConsumerImpl)
        sc.addSparkListener(jem)



        val tableList = List("call_center", "catalog_returns", "catalog_page", "catalog_sales",
                            "customer", "customer_address", "customer_demographics",
                            "date_dim", "dbgen_version", "household_demographics",
                            "income_band", "inventory", "item", "promotion", "reason",
                            "ship_mode", "store", "store_returns", "store_sales",
                            "time_dim", "warehouse",
                            "web_page", "web_returns", "web_sales", "web_site")
        val compressionCodecList = List("gzip", "lz4", "snappy", "zstd")
        val dstBucket = "<your-bucket-name>"
        val dstPath = "<path/to>"


        val glueMeasurement = new GlueMeasurement(spark, dstBucket, dstPath)
        glueMeasurement.generateTpcdsDataset(tableList)
        glueMeasurement.measureCompressionDecompressionSpeed(tableList, compressionCodecList)
        glueMeasurement.measureCompressionRatio(tableList, compressionCodecList, "none")

        sc.removeSparkListener(jem)
    }
}
