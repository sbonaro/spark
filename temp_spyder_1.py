# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# import package

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local")\
        .appName("test")\
            .getOrCreate()
            
data = [
('James', '', 'Smith','1991-04-01', 'M', 3000),
('Michael', 'Rose', '','200-05-19', 'M', 4000),
('Robert', '', 'Williams','1978-09-05', 'M', 4000),
('Maria', 'Anne', 'Jones','1976-12-01', 'F', 4000),
('Jen', 'Mary', 'Brown','1980-02-17', 'F', -1),
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

df = spark.createDataFrame(data= data, schema= columns)

# print(df.printSchema())
print(df)
