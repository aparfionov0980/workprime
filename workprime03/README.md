### Overview
Workprime03 is the modified version of Workprime02 with the following features:
* New argument should be specified: `recordsSize` which goes after `table`
* Input data now is repartitioned by the column `year`
* Reading data with db2 database now goes with `n` parallel connections. For specifying `n`
new Spark configuration parameter `spark.db2_partition_num` is added.

All Requirements and How To Run instructions are the same from Workprime02. 