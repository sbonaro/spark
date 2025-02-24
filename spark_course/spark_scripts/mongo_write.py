import os 
os.environ['SPARK_HOME'] = "/spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = "jupyter"
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = "lab"
os.environ['PYSPARK_PYTHON'] = "python"

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = int(fields[2]), occupation = int(fields[3], zip = int(fields[4])))

if __name__ == '__main__':

    # Create SparkSession
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Build RDD on top of users date file
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/mongodb/movies.user")

    # Creating new RDD by passing the parser function
    users = lines.map(parseInput)

    # Convert RDD into a DataFrame
    usersDataset = spark.createDataFrame(users)

    # Write the data into MongoBD
    usersDataset.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/moviesdate.users")\
        .mode("append")\
        .save("")
