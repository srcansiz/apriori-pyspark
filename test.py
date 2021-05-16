
from pyspark import SparkContext
from papriori import Apriori

sc = SparkContext("local",    "First App")


# Test data set, data set should be formatted as comma separated txt file without headers
path = "test-data.txt"

# Construct Apriori
apriori = Apriori(path, sc, minSupport="auto")

# Create model
apriori.fit()

# Make prediction for Apple
pr = apriori.predict(['Apple'], 40)

# Collect to list
result = pr.collect()

# Print result
print(result)

