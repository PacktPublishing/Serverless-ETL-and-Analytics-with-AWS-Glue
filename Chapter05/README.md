# Chapter 5: Data layouts

Here're sample data and scripts that we used through this chapter:
* `data.json`
* `compression_by_dataframe.py`
* `partitioning_by_dataframe.py`
* `MeasureCompressionSpeedAndRatio.scala`

## Appendix: Measuring compression speed and ratio with a AWS Glue Spark job
You can measure compression speed and ratio by running `MeasureCompressionSpeedAndRatio.scala` on AWS Glue. This approximately takes **6-7hours** for a 10-DPUs Glue Spark job. To run the measurement job, you need to set up [TPC-DS Glue custom connector](https://aws.amazon.com/marketplace/pp/prodview-xtty6azr4xgey) that generates the TPC-DS dataset for the measurement, and create a Glue Spark job that measures the compression speed and ratio. 
You can set up the connector and create a Glue job by going through [Set up the TPC-DS connector and run a Glue ETL job with the connector](https://github.com/aws-samples/aws-glue-samples/tree/master/GlueCustomConnectors/development/Spark/glue-3.0/tpcds-custom-connector-for-glue3.0#set-up-the-tpc-ds-connector-and-run-a-glue-etl-job-with-the-connector) section in [aws-glue-samples](https://github.com/aws-samples/aws-glue-samples) Github repository.

One of the measurement results is shown in *Table 5.1 – Comparison of compression ratio and speed between compression* in **Chapter 5.  Data layouts**.

