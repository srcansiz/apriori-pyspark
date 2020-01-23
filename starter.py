from pyspark.sql import SparkSession
from pyspark import SparkContext
from operator import add

sc = SparkContext("local" ,    "First App")

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
counts = words.count()
#print('Count is  : ' , counts  )

file =  sc.textFile("text.csv") 
            # .map(lambda line: line.split(",")) \
            # .filter(lambda line: len(line)<=1) \
            # .collect()
#print(file.collect())
#file = file.map(lambda line: line.split(","))
def sumOparator(x,y):
    return x+y

print(file.count())

flattedRecords = file.flatMap(lambda x: x.split(","))
keysOfRecords = flattedRecords.map(lambda word : (word ,1)).keys().distinct()
countsOfRecords = flattedRecords.map(lambda word : (word ,1)).reduceByKey(sumOparator)
minValue = countsOfRecords.min()[1]

filteredRecords = countsOfRecords.filter(lambda s:s[1] > minValue)
 
# Crate single array RDD from keyss
filteredRecords = filteredRecords.map(lambda record: ([record[0]], record[1]))
filteredRecords = filteredRecords.map(lambda record: record[0])

## Combination of all records
cartesian = filteredRecords.cartesian(filteredRecords)

#print(cartesian.collect())

print(cartesian.coalesce(2).collect())
# print(filteredRecords.collect())
# print(filteredRecords.cache().collect())

# print(keysOfRecords.collect())
# print(countsOfRecords.collect())
# print(countsOfRecords.min()[1])


#print(file.zipWithIndex().collect())


