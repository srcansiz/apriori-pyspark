# Apriori Algorithm with PySpark Implementation

This module is developed to run apriori algorithm on RDD based pyspark.

### Data Set 
Until feature update, data should be presented with comma separated 
txt file. It should be indicated in a constructor with its path. 
Every line should include raw item sets. Please see test-data.csv as an example. 

### Creating Apriori Model

To create model, please first import SparkContext and 
Apriori class which is created in the file papriori.py.
Minimum support value should be more than 2. If min support value is set as ``auto`` `minSupport="auto""` 
algorithm will select min support value from the supports values in the first apriori table
which includes single item sets.

````buildoutcfg
from pyspark import SparkContext  
from papriori import Apriori  

sc = SparkContext("local",    "First App")


# Test data set, data set should be formatted as comma separated txt file without headers
path = "test-data.txt"

# Construct Apriori
apriori = Apriori(path, sc, minSupport=2)
apriori.fit()
````

You can also get support values of all item combinations from the result of `apriori.fit()`

````buildoutcfg
supports = apriori.fit()
print(supports.collect())
````

### Prediction

To make prediction over created apriori model please use ``predict`` method of 
the apriori object. The first argument should be a list that indicates the item set. 
The second argument should be the confidence threshold. 
The result will return a nested array that indicates frequent item patterns 
that has confidence more than or equaled to specified confidence values

````buildoutcfg
apriori.predict(["Aplle"] , 45)

# Returns
# [[['Apple'], ['Mango'], 58.333333333333336], [['Apple'], ['Banana'], 41.66666666666667]]]

````