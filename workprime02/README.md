### Overview
Workprime02 is an application that take data from db2 database, transform it in the specific way and then insert it in the Cloud Object Storage bucket. Data transformation proccess is discribed as follows:
* New column `yaer_purchase` is added in table schema
* `month_sales` columns are deleted from table schema
* For each row year_purchase column is calculated by aggregating `month_sales`

So the output data has the following schema:

product_id | product_group | years | jan_sales | ... | dec_sales
---------- | ------------- | ----- | --------- | ... | ---------

And the output data generates like this:

product_id | product_group | years | year_purchase
---------- | ------------- | ----- | -------------

### Requirements
To run Workprime01 on your machine consider this software to be installed:
* JRE 1.8 or higher
* SBT 1.2.8 or higher

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
Open Workprime02 directory. Run `sbt compile` task. After successful compilation type `sbt run {table_name} {bucket name} {format} {output}` into the terminal.
Arguments explonation:
* `{table name}` is the name of the table in db2 database
* `{bucket_name}` is the name of the bucket in Cloud Object Storage
* `{format}` is the file format in wich to write output data
* `{output}` is the output data name