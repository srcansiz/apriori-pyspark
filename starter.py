from pyspark.sql import SparkSession
from pyspark import SparkContext
from operator import add

sc = SparkContext("local" ,    "First App")

print('------ \n')

file =  sc.textFile("text.csv") 

def sumOparator(x,y):
    return x+y


# Seperates records with "," and get into one array
starter = file.map(lambda line: line.split(','))

print('-------------Starter-----------------')
print(starter.collect())
print('\n')

flattedRecords = file.flatMap(lambda x: x.split(","))



# Convert records to a key and assing a value as 1 such as ('x1' , 1) x1 is the key
convertedAsKeys = flattedRecords.map(lambda word : (word ,1))



# Get only the keys of elements
keys = convertedAsKeys.keys()

# Gets the uniques keys distinct function genarates unique values in RDD
uniquesKeys = keys.distinct()

# reduceByKey function gets keys values and do the oparation which is passed
# In this example reduceByKey sums all keys values which same 

countsOfRecords = convertedAsKeys.reduceByKey(sumOparator)

print('------- Converted As Keys')
print(convertedAsKeys.collect())
print('\n')


print('------- Reuce by Key - SUM oparation')
print(countsOfRecords.collect())
print('\n')


## Gets the mininmum value of RDD
minValue = countsOfRecords.min()[1]

print('------ Mininmum value ------')
print(minValue)
print('\n')

## Filter RDD elements that has value less thna min value 

filteredRecords = countsOfRecords.filter(lambda s:s[1] > minValue)

print('------ Les Than Min Value : ' , str(minValue) , '------')
print(filteredRecords.collect())
print('\n')


filteredRecords = filteredRecords.map(lambda record: (record[0] ,record[1]))

# Crate single RDD from keys such as (['x1'] , [x2] ..... )
# filteredRecords = filteredRecords.map(lambda record: ([record[0]], record[1]))
filteredRecords = filteredRecords.map(lambda record: record[0])

print('-----------After Removing MinValue elements------------')
print(filteredRecords.collect())
print('\n')


## Combination of all records
combined = filteredRecords.cartesian(filteredRecords)

print('------ Combinations of All Elements-------------------------')
print(combined.collect())
print('\n')


## Remove If the Values are same in pair RDD's such as (['x2'] , ['x2'])
tmp = []
def deneme(record):
    
    x1 = record[0]
    x2 = record[1]
    
    tmp.append((x2,x1))
    #print(tmp)

    if(x1 != x2):
        if(tmp.count((x1 , x2)) == 0):
            return (x1 , x2)
        else:
            return [x1]
    else:
        return [x1]

def deneme2(record):

    x1 = record[0]
    x2 = record[1]
    





newOne = combined.map(lambda record: deneme(record))
newOne = newOne.filter(lambda x : len(x) == 2  )

print(newOne.collect())
print('\n')

csupport = newOne.cartesian(starter)
print(csupport.collect())
print('\n')


# Filter data according to first input  
secondOne = csupport.filter(lambda x: all(val in x[1] for val in x[0] ))
secondOne = secondOne.map(lambda x: (x[0] , 1))
secondOne = secondOne.reduceByKey(sumOparator)
secondOne = secondOne.filter(lambda x: x[1] > 1)
newMap = secondOne.map(lambda x: x[0])

print(secondOne.collect())
print('\n')


countsOfRecords = countsOfRecords.union(secondOne)
print(countsOfRecords.collect())
print('\n')

thirdOne = newMap.cartesian(uniquesKeys)
print(thirdOne.collect())
print('\n')


#print(csupport.collect())

#print(secondOne.keys().collect())
#print(secondOne.distinct().collect())


#print(newOne.collect())
#print(newOne.count())
#print(newOne.distinct().count())

# secondOneCombined = newOne.cartesian(filteredRecords)
# secondOne = secondOneCombined.map(lambda record: deneme(record))
# secondOne = secondOne.filter(lambda x : len(x) == 2)
# print('------- Second Combined')
# print(secondOneCombined.collect())
# print('\n')

# print('---- Second Remove Replica ------')
# print(secondOne.collect())
# print('----- \n')



# print('----- COntrol -------')
# print(secondOne.map(lambda x : len(x)).collect())
# print(secondOne.collect())
# print(newOne.collect())

# while(combinedRecords.isEmpty() == False):

