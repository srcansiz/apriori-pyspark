from pyspark.sql import SparkSession
from pyspark import SparkContext
from operator import add
import time

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

    if(any(lem == record[1] for lem in record[0]) == False):
        return record[0][0] , record[0][1] , record[1] 
    else:
        return record[0]





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

forthOne = thirdOne.map(lambda x: deneme2(x))
forthOne = forthOne.filter(lambda x: len(x) > 2)
print(forthOne.collect())
print('\n')


forthOne = forthOne.cartesian(starter)
forthOne = forthOne.filter(lambda x: all(val in x[1] for val in x[0]))
forthOne = forthOne.map(lambda x: (x[0],1))
forthOne = forthOne.reduceByKey(sumOparator)
forthOne = forthOne.filter(lambda x: x[1] > 1)
#forthOne = forthOne.map(lambda)

print(forthOne.collect())
print('---\n')
print(forthOne.isEmpty())
print('---\n')


#### -----------------------------------------------------------------------------------------


def removeReplica(record):

    if(isinstance(record[0], tuple)):
        x1 = record[0]
        x2 = record[1]
    else:
        x1 = [record[0]]
        x2 = record[1]

    if(any(x == x2 for x in x1) == False):
        a = list(x1)
        a.append(x2)
        a.sort()
        result = tuple(a)
        return result 
    else:
        return x1




file =  sc.textFile("text.csv")

## Whole Date set with frequencies  
lblitems = file.map(lambda line: line.split(','))


## Whole lines in single array 
wlitems = file.flatMap(lambda line:line.split(','))

## Unique frequent items in dataset
uniqueItems = wlitems.distinct()


supportRdd = wlitems.map(lambda item: (item , 1))
supportRdd = supportRdd.reduceByKey(sumOparator)
supports = supportRdd.map(lambda item: item[1])

print('-------SUPOORTS---------------')
print(supportRdd.collect())
print('----\n')
# Define minimum support value 
minSupport = supports.min()

# If mininmum support is 1 then replace it with 2 
minSupport = 1 if minSupport == 1 else minSupport

## Filter first supportRdd with minimum support 
supportRdd = supportRdd.filter(lambda item: item[1] >= minSupport )

## Craete base RDD with will be updated every iteration
baseRdd = supportRdd

supportRdd = supportRdd.map(lambda item: item[0])
supportRddCart = supportRdd

print(supportRdd.collect())

print(minSupport)

c = 2

while(supportRdd.isEmpty() == False):

    combined = supportRdd.cartesian(uniqueItems)
    combined = combined.map(lambda item: removeReplica(item))
  
    combined = combined.filter(lambda item: len(item) == c)
    combined = combined.distinct()

    
    combined_2 = combined.cartesian(lblitems)
    combined_2 = combined_2.filter(lambda item: all(x in item[1] for x in item[0]))
    
    combined_2 = combined_2.map(lambda item: item[0])
    combined_2 = combined_2.map(lambda item: (item , 1))
    combined_2 = combined_2.reduceByKey(sumOparator)
    combined_2 = combined_2.filter(lambda item: item[1] >= minSupport)
    baseRdd = baseRdd.union(combined_2)
    
    combined_2 = combined_2.map(lambda item: item[0])
    supportRdd = combined_2
    print('-----RESUTLS-------')
    print(combined_2.collect())
    print('\n')
    time.sleep(2)
    c = c+1 

print(baseRdd.collect())

### Confidence Calculation 

