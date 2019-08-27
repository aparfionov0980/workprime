### Overview
Workprime02 is a spark application that take data from db2 database, transform it in the specific way and then insert it in the Cloud Object Storage bucket. Data transformation proccess is discribed as follows:
* New column `yaer_purchase` is added in table schema
* `month_sales` columns are deleted from table schema
* For each row year_purchase column is calculated by aggregating `month_sales`

So the output data has the following schema:

product_id | product_group | years | jan_sales | ... | dec_sales
---------- | ------------- | ----- | --------- | --- | ---------

And the output data generates like this:

product_id | product_group | years | year_purchase
---------- | ------------- | ----- | -------------

### Requirements
To run Workprime01 on your machine consider this software to be installed:
* JRE 1.8 or higher
* Spark 2.4.3 or higher

Following packages must be specified in `spark-submit` command:
* com.ibm.db2.jcc:db2jcc:db2jcc4,
* com.ibm.stocator:stocator:1.0.35.

Following parameters must be also set after `--conf` property:
- For db2 connection:
  - spark.db2_jdbc_url - url for the jdbc driver
  - spark.db2_username - db2 database username
  - spark.db2_password - db2 database password
- For Cloud Object Storage connection:
  - spark.cos_service_name - cos service name
  - spark.cos_endpoint - bucket endpoint url
  - spark.cos_access_key - access key generated in credentials
  - spark.cos_secret_key - secret key generated in credentials 

### How to run
To run Workprime02 use `spark-submit --packages {libs} {spark-params} {spark-conf} {path/to/jar} {table name} {bucket_name} {format} {output}`, where:
* `{libs}` - packages with needs to run application. Listed above
* `{spark-params}` - spark parameters like `name`, `master`
* `{spark-conf}` - arbitrary Spark configuration property in key=value format. All necessary properties are listed above.
* `{path/to/jar}` - path to the jar file
* `{table name}` - name of the table in db2 database
* `{bucket_name}` - name of the bucket in Cloud Object Storage
* `{format}` - file format in wich to write output data
* `{output}` - output data name
