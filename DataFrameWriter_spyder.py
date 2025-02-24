# En este caso el dataFrame no tiene headers

# import os 
# os.environ['SPARK_HOME'] = "/spark"
# os.environ['PYSPARK_DRIVER_PYTHON'] = "jupyter"
# os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = "lab"
# os.environ['PYSPARK_PYTHON'] = "python"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func


spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

mychema = StructType([\
                      StructField("userID", IntegerType(), True),
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

####### NUEVO CON RESPECTO AL READER

output = people.select(people.userID,people.name\
                        ,people.age,people.friends)\
        .where(people.age < 30).withColumn('insert_ts', func.current_timestamp())\
        .orderBy(people.userID)
        
output.createOrReplaceTempView("peoples")               # SOLO PARA MOSTRAR DATOS
#spark.sql("select userID, name from peoples").show()    # SOLO PARA MOSTRAR DATOS
#spark.sql("select * from peoples").show()               # SOLO PARA MOSTRAR DATOS

# CSV FORMAT
output.write\
.format("csv").mode("overwrite")\
.option("path", "C:/Users/cti7600/Desktop/Modelado_CU/PROG/APACHE/output/op/")\
.partitionBy("age")\
.save()

# PARQUET FORMAT
#output.write\
#.format("parquet").mode("overwrite")\ # parquet es default datasource para spark - es columnar
#.option("path", "C:/Users/cti7600/Desktop/Modelado_CU/PROG/APACHE/output/op/")\
#.partitionBy("age")\
#.save()

#JSON FORMAT
#output.write\
#.format("json").mode("overwrite")\
#.option("path", "C:/Users/cti7600/Desktop/Modelado_CU/PROG/APACHE/output/op/")\
#.partitionBy("age")\
#.save()


