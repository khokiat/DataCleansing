
"""***Dowload data***"""

#Dowload data
!wget -O data.zip https://file.designil.com/zdOfUE+

!unzip data.zip

#Load data to spark
df = spark.read.csv('/content/ws2_data.csv', header = True, inferSchema= True,)

"""# **Data profilling**"""

df #Check each column

df.show(10) #Check data

df.dtypes #Check data type each column

df.printSchema() #Also check data type each column

print((df.count(),len(df.columns))) #Count column and row

df.describe().show() #Summary statistical data

df.select("price",'country').describe().show() #Summary only specify column

df.summary('count').show() # overview checking null value

df.where('user_id is null').show() #df.where(df.user_id.isNull).show() and you can also use filter function

"""# **EDA**

# **Non-Graphical EDA**
"""

#Numeric data
df.where(df.price>=1).show()

#String data
df.where(df.country == 'Canada').show()

df.where(df.timestamp >= '2021-05-01').show()

df.where(df.timestamp.startswith('2021-05')).show()

"""# **Graphical**
This section will use seaborn matplotlib and pandas to plot graph instead.

"""

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

#Converse sparkdata frame to pandas dataframe
dt_pd = df.toPandas()

dt_pd.head()

sns.boxplot(x = dt_pd['book_id']) #case sensitive user lower case

sns.histplot(dt_pd['price'], bins=10) #overview of the most accumulate range

#scatter plot to see relation between price and book_id
sns.scatterplot(x = dt_pd.book_id, y = dt_pd.price)

#plotly - interactive chart

import plotly.express as px
fig = px.scatter(dt_pd, 'book_id', 'price')
fig.show()

"""# **Anomoly checking**

# **Convert data type**
"""

df.printSchema()

df.select('timestamp').show(10)

#convert string to datetime
#withColumn is a function to change data type.
# if same name, won't create new column.
from pyspark.sql import  functions as f

df_clean = df.withColumn("timestamp", f.to_timestamp(df.timestamp,'yyyy-MM-dd HH:mm:ss'))
df_clean.show()

df_clean.printSchema()

# Try to opereate with timestamp
df_clean.where((f.dayofmonth(df_clean.timestamp) <=15) & (f.dayofmonth(df_clean.timestamp) == 6)).count()

"""**Syntactical anomolies**"""

df_clean.select("country").distinct().count() # to see all country

#Sort() : to order data from A --> Z
#False is used to command show full table
df_clean.select("country").distinct().sort("country").show(50, False)

df_clean.where(df_clean.country == 'Japane').show()

df_clean_country = df_clean.withColumn('country_update',f.when(df_clean['country'] == 'Japane', 'Japan').otherwise(df_clean['Country']))

df_clean_country.select('country_update').distinct().sort('country_update').show(50)

df_clean = df_clean_country.drop('country').withColumnRenamed('country_update','country')

df_clean.show()

"""**semantic anomolies**"""

df_clean.select("user_id").count()

# Check pattern of used_id with regex101 ;https://regex101.com/
df_clean.where(df_clean['user_id'].rlike('^[a-z0-9]{8}$')).count()

df_correct_userid = df_clean.filter(df_clean["user_id"].rlike('^[a-z0-9]{8}$'))

df_incorrect_userid = df_clean.subtract(df_correct_userid)

df_incorrect_userid.show()

df_clean_userid = df_clean.withColumn('user_update',f.when(df_clean['user_id'] == 'ca86d17200', 'ca86d172').otherwise(df_clean['user_id']))
df_clean_userid.select('user_update').distinct().sort('user_update').show(50)

df_clean = df_clean_userid.drop('user_id').withColumnRenamed('user_update','user_id')

df_clean.show()

df_clean.where(df_clean['user_id'] == 'ca86d172').show() # Recheck

"""**Missing value**"""

# col = Spark command to select column
# sum = Spark command to summary
from pyspark.sql.functions import col, sum

df_nulllist = df_clean.select([ sum(col(colname).isNull().cast("int")).alias(colname) for colname in df_clean.columns ])
df_nulllist.show()

df_clean.summary("count").show() #Recheck

#Change null to 00000000 just example don't do for real case
df_clean_null = df_clean.withColumn('user_update',f.when(df_clean['user_id'].isNull(), '00000000').otherwise(df_clean['user_id']))
df_clean = df_clean_null.drop('user_id').withColumnRenamed('user_update','user_id')
df_clean.where( df_clean.user_id.isNull() ).show() # Recheck
df_clean.summary("count").show() #Recheck

"""**Out lier checking**"""

df_clean_pd = df_clean.toPandas()

sns.boxplot(x= df_clean_pd['price'])

# Recheck in system for the book that price is over 80 dallar.
df_clean.where(df_clean.price>80).select('book_id').distinct().show()

"""**Save file**"""

#Write as 1 file
df_clean.coalesce(1).write.csv('Cleandata.csv', header = True)

