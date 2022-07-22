from awsglue.context import GlueContext
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())
logger = glueContext.get_logger()
logger.info("info log message")
logger.warn("warn log message")
logger.error("error log message")
