### Overview
Workprime01 creates table in db2 database, generate data then insert into the created table. Table schema consist of 12 columns and is represented in the following way:
  
product_id | product_group | years | month_sales
---------- | ------------- | ----- | -----------

where month_sales is a replacement for the 12 months columns (jan_sales, mar_sales etc.).
Amount of data, table name and connection credits are user defined.

Generated data also satisfies folowing requirements:
* There are no duplicates for the same product/year
* For multiple product rows for different years, product/group combination is concise

### Requirements
To run Workprime01 on your machine consider this software to be installed:
* JRE 1.8 or higher
* SBT 1.2.8 or higher

Following environmental variables must be also set in your system:
* DB2_JDBC-URl - url for the jdbc driver
* DB2_USERNAME - db2 database username
* DB2_PASSWORD - db2 database password

### How to run
Open Workprime01 directory. Run `sbt compile` task. After successful compilation type `sbt run {table_name} {records_size}` into the terminal
where `{table name}` is a name for the table and `{record_size}` is a number of records to be generated. In case of successful work the last output row
should be like this `Records inserted`