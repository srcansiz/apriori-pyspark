from pyspark.sql import SparkSession
from pyspark import SparkContext
from operator import add
import time

sc = SparkContext("local" ,    "First App")


def sumOparator(x,y):
    return x+y


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

# Define minimum support value 
minSupport = supports.min()

# If mininmum support is 1 then replace it with 2 
minSupport = 1 if minSupport == 1 else minSupport

## Filter first supportRdd with minimum support 
supportRdd = supportRdd.filter(lambda item: item[1] >= minSupport )

## Craete base RDD with will be updated every iteration
baseRdd = supportRdd.map(lambda item: ([item[0]] , item[1])) 
print('1 . Table has crated...') 

supportRdd = supportRdd.map(lambda item: item[0])
supportRddCart = supportRdd


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
    print(c ,'. Table has crated... ')
    c = c+1 


### Confidence Calculation 

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r %s %s |%s| %s%% %s' % ('Preprocessing ' , prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()





# Filter catersian RDD according to conditions 
# Condition 1- If both tuple elements lenght are same 
# Condition 2- Check it is a tuple or single str object (which first table that show single items supports)

class Filter():

    def __init__(self):
        
        self.stages = 1


    def filterForConf(self, item , total):
        
        if(len(item[0][0]) > len(item[1][0])  ):
            if(self.checkItemSets(item[0][0] , item[1][0]) == False):
                pass
            else:
                return (item)       
        else:
            pass  
        self.stages = self.stages + 1

    # Check Items sets includes at least one comman item // Example command: # any(l == k for k in z for l in x )
    def checkItemSets(self, item_1 , item_2):

        if(len(item_1) > len(item_2)):
            return all(any(k == l for k in item_1 ) for l in item_2)
        else:
            return all(any(k == l for k in item_2 ) for l in item_1)


    def calculateConfidence(self, item):

        # Parent item list
        parent = set(item[0][0])
        
        # Child item list
        if(isinstance(item[1][0] , str)):
            child  = set([item[1][0]])
        else:
            child  = set(item[1][0])
        # Parent and Child support values
        parentSupport = item[0][1]
        childSupport = item[1][1]
        # Finds the item set confidence is going to be found

        support = (parentSupport / childSupport)*100

        return list([ list(child) ,  list(parent.difference(child)) , support ])

        
# Example ((('x10', 'x3', 'x6', 'x7', 'x9'), 1), (('x10', 'x3', 'x7'), 1))
calcuItems = baseRdd.cartesian(baseRdd)

# Create Filter Object
ff = Filter()

#deneme = calcuItems.map(lambda item: lens(item)) 
total = calcuItems.count()

print('# : Aggregated support values preparing for the confidence calculatations')
deneme = calcuItems.filter(lambda item: ff.filterForConf(item , total))
deneme = deneme.map(lambda item: ff.calculateConfidence(item))
print('# : Aggregated support values are ready !')



    
print(deneme.collect())
