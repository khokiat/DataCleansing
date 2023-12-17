# DataCleansing
+ Using apache spark and matplotlib to overview data and clean data.
+ This practice was run on google colab

## Installing Apache spark
### Update apackage in VM

``` 
!apt-getupdate # Update all package in VM
!apt-get install openjdk-8-jdk-headless -qq > /dev/null # Install Java Development Kit
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz #Dowload 
```
### Install Spark 3.1.2
!tar xzvf spark-3.1.2-bin-hadoop2.7.tgz                                                  # Unzip Spark 3.1.2 file
!pip install -q findspark==1.3.0
### Set enviroment variable Python and Spark
```
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"
```
### Install PySpark into Python
To interact with spark with python
```
!pip install pyspark==3.1.2
```
### Create session for using spark
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
```
