# Serverless-ETL-and-Analytics-with-AWS-Glue

Serverless ETL and Analytics with AWS Glue, Published by Packt

## Chapter 04 - Data Preparation

### Sample Data

The sample data used in this chapter is being generated within the same script where necessary.

### Code execution and testing

- Code snippets in this chapter can be executed using different methods - AWS Glue ETL Jobs, AWS Glue Interactive Sessions, AWS Glue Development Endpoints, AWS Glue Studio Notebooks, AWS Glue Local Development Libraries or AWS Glue Docker image.
- Depending on the method used, you may incur some cost. You can keep the cost minimal by using AWS Glue Local Development libraries or AWS Glue ETL Docker images to execute the workload on your workstation. However, some features (Ex: Job bookmarks, `glueparquet` writer) are only available on AWS Glue job system. Refer [Developing and testing AWS Glue job scripts - Local Development Restrictions](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#local-dev-restrictions) for more information.
- If you are using compute/storage resources on AWS Cloud, please make sure to terminate any resources that you create once testing is done.
- Exercise caution while provisioning resources like IAM principals, Security groups, Network ACLs. Please make sure to follow `The principle of least privilege` and only assign permissions which are necessary for your workload.
