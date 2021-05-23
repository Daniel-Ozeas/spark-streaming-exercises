from os.path import abspath

from pyspark.sql import SparkSession

logFile = "/home/workspace/data/Test.txt"


spark = (SparkSession.builder
            .appName("HelloSpark")
            .getOrCreate()
        )

spark.sparkContext.setLogLevel("WARN")

logData = spark.read.text(logFile)
# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
numAs = logData.filter(logData.value.contains("a")).count()
# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
numsBs = logData.filter(logData.value.contains("b")).count()

print("********************************")
print("********************************")
print(f"Lines with A: {numAs}, lines with B: {numsBs}")
print("********************************")
print("********************************")

# TO-DO: stop the spark application
spark.stop
