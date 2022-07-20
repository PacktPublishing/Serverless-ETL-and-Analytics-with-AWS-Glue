import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaSparkContext
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator
import org.apache.hudi.utilities.UtilHelpers
import com.amazonaws.services.glue.AWSGlueClient
import com.amazonaws.services.glue.model.GetConnectionRequest

object GlueApp {
  def main(sysArgs: Array[String]) {

  val args = GlueArgParser.getResolvedOptions(
      sysArgs, Seq("JOB_NAME","TARGET_BUCKET","CONFIG_BUCKET").toArray)
  
  val glueClient = AWSGlueClient.builder().build()
  val getConnRequest = new GetConnectionRequest().withName("chapter-data-analysis-msk-connection").withHidePassword(true)
  val getConnResult =  glueClient.getConnection(getConnRequest)
  val bootstrapServers = getConnResult.getConnection().getConnectionProperties().get("KAFKA_BOOTSTRAP_SERVERS")

  var config = Array(
        "--table-type" , "COPY_ON_WRITE",
        "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider", 
        "--source-class", "org.apache.hudi.utilities.sources.JsonKafkaSource",
        "--source-ordering-field", "record_creation_time", 
        "--target-base-path", "s3://"+ args("TARGET_BUCKET") + "/hudi/employees_deltastreamer/", 
        "--target-table", "employees_deltastreamer", 
        "--op", "UPSERT",
        "--enable-sync", 
        "--hoodie-conf", "hoodie.deltastreamer.schemaprovider.source.schema.file=s3://" + args("CONFIG_BUCKET") + "/HandsonSeriesWithAWSGlue/config/employees.avsc", 
        "--hoodie-conf", "hoodie.deltastreamer.schemaprovider.target.schema.file=s3://" + args("CONFIG_BUCKET") + "/HandsonSeriesWithAWSGlue/config/employees.avsc", 
        "--hoodie-conf", "hoodie.deltastreamer.source.kafka.topic=chapter-data-analysis", 
        "--hoodie-conf", "hoodie.datasource.hive_sync.table=employees_deltastreamer",
      "--hoodie-conf", "hoodie.datasource.hive_sync.database=chapter-data-analysis-glue-database",
        "--hoodie-conf", "hoodie.datasource.write.recordkey.field=emp_no", 
        "--hoodie-conf", "hoodie.datasource.hive_sync.use_jdbc=false",
    "--hoodie-conf", "hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor",
        "--hoodie-conf", "hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "--hoodie-conf", "hoodie.cleaner.policy=KEEP_LATEST_COMMITS",
    "--hoodie-conf", "hoodie.cleaner.commits.retained=3",
        "--hoodie-conf", "security.protocol=SSL", 
        "--hoodie-conf", "auto.offset.reset=earliest", 
        "--hoodie-conf", "bootstrap.servers=" + bootstrapServers, 
        "--continuous"
    )

val cfg = HoodieDeltaStreamer.getConfig(config)
val additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg)
val jssc = UtilHelpers.buildSparkContext("delta-streamer-test", "jes", additionalSparkConfigs)

   val spark = jssc.sc
   val glueContext: GlueContext = new GlueContext(spark)
   Job.init(args("JOB_NAME"), glueContext, args.asJava)

try {
      new HoodieDeltaStreamer(cfg, jssc).sync();
    } finally {
      jssc.stop();
    }

  
    Job.commit()
  }
}