#!/usr/bin/env python
# coding: utf-8

# # Exercise 3 - Data Lake on S3

# In[4]:


from pyspark.sql import SparkSession
import os
import configparser


# # Make sure that your AWS credentials are loaded as env vars

# In[6]:


config = configparser.ConfigParser()

#Normally this file should be in ~/.aws/credentials
config.read_file(open('aws/credentials.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


# # Create spark session with hadoop-aws package

# In[3]:


spark = SparkSession.builder                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")                     .getOrCreate()


# # Load data from S3

# In[4]:


df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv")


# In[5]:


df.printSchema()
df.show(5)


# # Infer schema, fix header and separator

# In[10]:


df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=";", inferSchema=True, header=True)


# In[11]:


df.printSchema()
df.show(5)


# # Fix the data yourself 

# In[23]:


import  pyspark.sql.functions as F
dfPayment = df.withColumn("payment_date", F.to_timestamp("payment_date"))
dfPayment.printSchema()
dfPayment.show(5)


# # Extract the month

# In[24]:


dfPayment = dfPayment.withColumn("month", F.month("payment_date"))
dfPayment.show(5)


# # Computer aggregate revenue per month

# In[27]:


dfPayment.createOrReplaceTempView("payment")
spark.sql("""
    SELECT month, sum(amount) as revenue
    FROM payment
    GROUP by month
    order by revenue desc
""").show()


# # Fix the schema

# In[34]:


from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
paymentSchema = R([
    Fld("payment_id",Int()),
    Fld("customer_id",Int()),
    Fld("staff_id",Int()),
    Fld("rental_id",Int()),
    Fld("amount",Dbl()),
    Fld("payment_date",Date()),
])


# In[35]:


dfPaymentWithSchema = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=";", schema=paymentSchema, header=True)


# In[37]:


dfPaymentWithSchema.printSchema()
df.show(5)


# In[39]:


dfPaymentWithSchema.createOrReplaceTempView("payment")
spark.sql("""
    SELECT month(payment_date) as m, sum(amount) as revenue
    FROM payment
    GROUP by m
    order by revenue desc
""").show()

