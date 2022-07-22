# Chapter 09 sample code

product_customer_sales.json
```
{"product_name":"Best practices on data lakes","category":"Ebooks","price":25,"customer_name":"Tanya Fowler","email":"tanya@example.net","phone":"(067)150-0263","purchased_at":"2022-04-28T08:20:00Z"}
{"product_name":"Data Quest","category":"Video games","price":30,"customer_name":"Rebecca Thompson","email":"thompson@example.net","phone":"001-469-964-3897x9041","purchased_at":"2022-03-30T01:30:00Z"}
{"product_name":"Final Shooting","category":"Video games","price":20,"customer_name":"Rachel Gilbert","email":"gilbert@example.com","phone":"001-510-198-4613x23986","purchased_at":"2022-04-01T02:00:00Z"}
```

```
$ BUCKET_NAME="simple-datalake-<your-producer-account-id>"
```

```
$ aws s3api create-bucket --bucket ${BUCKET_NAME} --create-bucket-configuration LocationConstraint=us-west-2
```

```
$ aws s3 cp product_customer_sales.json s3://${BUCKET_NAME}/simple_datalake/pcs/
```

```
CREATE DATABASE simple_datalake
```

```
CREATE EXTERNAL TABLE simple_datalake.pcs(
  product_name string, 
  category string, 
  price int,
  purchased_at string,
  customer_name string, 
  email string, 
  phone string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple-datalake-<your-producer-account-id>/simple_datalake/pcs/'
TBLPROPERTIES ('classification'='json')
```

```
SELECT * FROM simple_datalake.pcs
```

```
{ 
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetTableVersion",
                "glue:GetTableVersions",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchGetPartition",
                "glue:SearchTables"
            ],
            "Principal": {
                "AWS": [
                    "arn:aws:iam::<your-consumer-account-id>:root"
                ]
            },
            "Resource": [
                "arn:aws:glue:us-west-2:<your-producer-account-id>:table/simple_datalake/pcs",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:database/simple_datalake",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:catalog"
           ]
        }
    ]
}
```

```
$ aws glue put-resource-policy --policy-in-json file://./catalog-policy.json --enable-hybrid TRUE --region us-west-2
```

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::<your-consumer-account-id>:root"
                ]
            },
            "Action": [
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::simple-datalake-<your-producer-account-id>/simple_datalake/pcs/*"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::<your-consumer-account-id>:root"
                ]
            },
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::simple-datalake-<your-producer-account-id>"
        }
    ]
}

```

```
$ aws s3api put-bucket-policy --bucket simple-datalake-<your-producer-account-id> --policy file://./bucket-policy.json
```

product_customer_sales.json
```
{"product_name":"Introduction to Cloud","category":"Ebooks","price":15,"customer_name":"Barbara Gordon","email":"gordon@example.com","phone":"117.835.2584","purchased_at":"2022-04-21T11:40:00Z"}
{"product_name":"Best practices on data lakes","category":"Ebooks","price":25,"customer_name":"Tanya Fowler","email":"tanya@example.net","phone":"(067)150-0263","purchased_at":"2022-04-28T08:20:00Z"}
{"product_name":"Data Quest","category":"Video games","price":30,"customer_name":"Rebecca Thompson","email":"thompson@example.net","phone":"001-469-964-3897x9041","purchased_at":"2022-03-30T01:30:00Z"}
{"product_name":"Final Shooting","category":"Video games","price":20,"customer_name":"Rachel Gilbert","email":"gilbert@example.com","phone":"001-510-198-4613x23986","purchased_at":"2022-04-01T02:00:00Z"}
```

```
$ BUCKET_NAME="standard-datalake-<your-producer-account-id>"
$ aws s3api create-bucket --bucket ${BUCKET_NAME} --create-bucket-configuration LocationConstraint=us-west-2
```

```
CREATE DATABASE standard_datalake
```

```
CREATE EXTERNAL TABLE standard_datalake.pcs(
  product_name string, 
  category string, 
  price int,
  purchased_at string,
  customer_name string, 
  email string, 
  phone string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://standard-datalake-<your-producer-account-id>/standard_datalake/pcs/'
TBLPROPERTIES ('classification'='json')
```

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*"
            ],
            "Principal": {
                "AWS": [
                    "arn:aws:iam::<your-consumer-account-id>:root"
                ]
            },
            "Resource": [
                "arn:aws:glue:us-west-2:<your-producer-account-id>:table/*",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:database/*",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:catalog"
            ],
            "Condition": {
                "Bool": {
                    "glue:EvaluatedByLakeFormationTags": true
                }
            }
        }
    ]
}
```

```
$ aws glue put-resource-policy --policy-in-json file://./catalog-policy-lf.json --enable-hybrid TRUE --region us-west-2
```

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetTableVersion",
                "glue:GetTableVersions",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchGetPartition",
                "glue:SearchTables"
            ],
            "Principal": {
                "AWS": [
                    "arn:aws:iam::<your-consumer-account-id>:root"
                ]
            },
            "Resource": [
                "arn:aws:glue:us-west-2:<your-producer-account-id>:table/simple_datalake/pcs",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:database/simple_datalake",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:catalog"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:*"
            ],
            "Principal": {
                "AWS": [
                    "arn:aws:iam::<your-consumer-account-id>:root"
                ]
            },
            "Resource": [
                "arn:aws:glue:us-west-2:<your-producer-account-id>:table/*",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:database/*",
                "arn:aws:glue:us-west-2:<your-producer-account-id>:catalog"
            ],
            "Condition": {
                "Bool": {
                    "glue:EvaluatedByLakeFormationTags": true
                }
            }
        }
    ]
}
```

```
SELECT * FROM standard_datalake_consumer.pcs_link
```