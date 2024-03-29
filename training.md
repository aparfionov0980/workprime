# Data Engineer / Scala

As a **Data Engineer**, you’ll be managing data pipelines for companies that deal with large volumes of data. That means making sure that your data is being efficiently collected and retrieved from its source when needed, cleaned and preprocessed.

**Required skills:**
- Strong programming skills in Java and/or Scala
- Solid foundation in SQL
- Experience working with Apache Spark
- Cloud containerization environments and micro-services: Docker, Kubernetes
- Experience working with Apache Airflow

**Desired skills:**
- Hadoop and/or Hive
- Stream-processing software: Kafka, RabbitMQ, MQTT, Apache Spark Streaming
- noSQL databases: ElasticSearch, Cloudant

## Big Data Foundations
- Complete the following course to get essential understanding about Big Data and Hadoop
  - Big Data and Hadoop Essentials - https://www.udemy.com/big-data-and-hadoop-essentials-free-tutorial

## Scala + Apache Spark
- Complete the courses to start with base knowledge of Scala and Apache Spark
  - https://cognitiveclass.ai/learn/scala/
  - https://www.udemy.com/scala-and-spark-2-getting-started

## IBM Cloud + Apache Spark
- Create IBM Cloud account - https://cloud.ibm.com/registration
- Create an instance of Analytics Engine service - https://cloud.ibm.com/catalog/services/analytics-engine

### Training Application - Data Load
- Create an account on Github and share it with your supervisor
- Create an instance of DB2 service on the cloud
- Create a Scala application that
  - Connects to the DB2 instance on the cloud
  - Generates a table with 20k of sample records
    - *product_id* - autogenerated numeric
    - *product_group* - autogenerated numeric in 0..9 range
    - *year* - autogenerated numeric in 2015..2018 range
    - 12 columns with monthly purchases amount - numeric in 0..100000 range
  - There should be no duplicates for the same product/year
  - If there are multiple product rows for different years, product/group combination should be concise
- Application should be built using **sbt** tool
- Use environment variables to specify configuration parameters like JDBC URL and connection credentials
- Add description of the application functionality and instructions on how to run it into a *README.md* file in Github

### Training Application - Data Transformation
- Create a data tranformation application using Apache Spark
- Read data from the DB2 on the cloud (use the service instance and data from the previous task) 
- Aggregate data in the dataframe by calculating total purchases amount per year
  - For each row calculate total of monthly purchases
  - Save the year total as a new column *year_purchases*
  - Remove the columns with monthly amounts from the data frame
- Save the modified dataframe as a file in Cloud Object Storage - https://www.ibm.com/cloud/object-storage
  - Use Stocator library - https://github.com/CODAIT/stocator
- Application should be built using **sbt** tool and published to Github
- Use *spark-submit* to run the application and Spark configuration properties to specify configuration parameters like JDBC URL and connection credentials
  - Submitting Applications - https://spark.apache.org/docs/latest/submitting-applications.html
  - Method to get Spark configuration properties - https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/SparkContext.html#getConf--
- Add description of the application functionality and instructions on how to run it using *spark-submit* into a *README.md* file in Github

### Training Application - Data Partitioning
- Modify the data transformation application to support data partitioning
  - Spark Documentation - https://spark.apache.org/docs/2.2.1/sql-programming-guide.html#jdbc-to-other-databases
- Partition data by *year* value and play with number of executors/workers to see the effects
- Useful links
  - Tips for using JDBC in Apache Spark SQL - https://medium.com/@radek.strnad/tips-for-using-jdbc-in-apache-spark-sql-396ea7b2e3d3
  - How to optimize partitioning when migrating data from JDBC source? - https://stackoverflow.com/questions/52603131/how-to-optimize-partitioning-when-migrating-data-from-jdbc-source
  - How to improve performance for slow Spark jobs using DataFrame and JDBC connection? - https://stackoverflow.com/questions/32188295/how-to-improve-performance-for-slow-spark-jobs-using-dataframe-and-jdbc-connecti
- Application should be built using **sbt** tool and published to Github
- Add description of the application functionality and instructions on how to run it using *spark-submit* into a *README.md* file in Github

### Training Application - Unit Tests
- Add unit tests to the training application
  - For unit tests creation use *ScalaTest* framework - http://www.scalatest.org/user_guide
  - Tests style *org.scalatest.FunSpec* - http://www.scalatest.org/getting_started_with_fun_spec
  - Mocks for external connections *org.scalamock.scalatest.MockFactory* - https://scalamock.org/
  - Utility to compare dataframes *com.github.mrpowers.spark.fast.tests.DataFrameComparer* - https://github.com/MrPowers/spark-fast-tests
- Include unit tests into the **sbt** build script

## Docker + Kubernetes
- Complete the following courses
  - Docker Essentials: A Developer Introduction - https://cognitiveclass.ai/courses/docker-essentials/
  - Container & Kubernetes Essentials with IBM Cloud - https://cognitiveclass.ai/courses/kubernetes-course/
- Look at the best practices
  - Best practices for writing Dockerfiles - https://docs.docker.com/develop/develop-images/dockerfile_best-practices/
  
## Spark + Kubernetes
- Study the Apache Spark documentation
  - Running Spark on Kubernetes - https://spark.apache.org/docs/latest/running-on-kubernetes.html
  
## Minikube
- Install Minikube on your local PC.
  - Use the following guide to install kubectl https://kubernetes.io/docs/tasks/tools/install-kubectl/
  - Use the following guide to install minikube https://kubernetes.io/docs/tasks/tools/install-minikube/
- Complete the following tutorial https://kubernetes.io/docs/tutorials/kubernetes-basics/
- Run your test application on minikube
  - Create deployment out of your test application
  
## Apache Airflow
- Familiarize yourself with Apache Airflow 
  - Documentation - https://airflow.apache.org/
  - Getting started with Apache Airflow - https://towardsdatascience.com/getting-started-with-apache-airflow-df1aa77d7b1b
  - Airflow tutorial videos - https://www.youtube.com/playlist?list=PLYizQ5FvN6pvIOcOd6dFZu3lQqc6zBGp2
  - Airflow: Lesser Known Tips, Tricks, and Best Practises - https://medium.com/datareply/airflow-lesser-known-tips-tricks-and-best-practises-cf4d4a90f8f
- Create a sample DAG via UI for a Spark submit and run it
- Create a Python script defining the same DAG programatically via Airflow API and run it on Airflow

For quick overview of how Airflow clusters work look at these two articles:
- How Apache Airflow Distributes Jobs on Celery workers - https://blog.sicara.com/using-airflow-with-celery-workers-54cb5212d405
- Setting up an Apache Airflow Cluster - http://site.clairvoyantsoft.com/setting-apache-airflow-cluster/

## Optional Training
- Complete the following learning paths
  - Hadoop Fundamentals - https://cognitiveclass.ai/learn/hadoop/
  - Hadoop Programming - https://cognitiveclass.ai/learn/big-data-hadoop-programming/
  - Hadoop Data Access - https://cognitiveclass.ai/learn/big-data-storage-and-retrieval/
