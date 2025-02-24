# -*- coding: utf-8 -*-
"""
Created on Sun Jun 16 21:31:47 2024

@author: CTI7600
"""

# En este caso el dataFrame no tiene headers

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func


spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

mychema = StructType([\
                      StructField("UserId", IntegerType(), True),
                      StructField("name",StringType() ,True),
                      StructField("age", IntegerType(), True),
                      StructField("friends", IntegerType(), True),
                      ])
    
people = spark.read.format("csv")\
    .schema(mychema)\
    .option("inferSchema", "False")\
    .option("path", "C:/Users/cti7600/Desktop/Modelado_CU/PROG/APACHE/fakefriends.csv")\
    .load()
    
people.printSchema()