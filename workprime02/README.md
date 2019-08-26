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

Following environmental variables must be also set in your system:
* For db2 connection:
 * DB2_JDBC-URl - url for the jdbc driver
 * DB2_USERNAME - db2 database username
 * DB2_PASSWORD - db2 database password
* For Cloud Object Storage connection:
 * COS_ENDPOINT - bucket endpoint url
 * COS_ACCESS_KEY - access key generated in credentials
 * COS_SECRET_KEY - secret key generated in credentials 

### How to run
To run Workprime02 use `spark-submit --packages {libs} {spark-params} {path/to/jar} {table name} {bucket_name} {format} {output}`, where:
* `{libs}` - packages with needs to run application. Listed above
* `{spark-params}` - spark parameters like `name`, `master`
* `{path/to/jar}` - path to the jar file
* `{table name}` - name of the table in db2 database
* `{bucket_name}` - name of the bucket in Cloud Object Storage
* `{format}` - file format in wich to write output data
* `{output}` - output data name
