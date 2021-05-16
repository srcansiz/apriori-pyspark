import utils as ut

class Apriori:

    def __init__(self, path, sc, minSupport=2):

        # File path
        self.confidences = None
        self.path = path

        # Spark Context
        self.sc = sc

        self.minSupport = minSupport
        self.raw = self.sc.textFile(self.path)

        ## Whole Date set with frequencies
        self.lblitems = self.raw.map(lambda line: line.split(','))

        ## Whole lines in single array
        self.wlitems = self.raw.flatMap(lambda line: line.split(','))

        ## Unique frequent items in dataset
        self.uniqueItems = self.wlitems.distinct()

    def fit(self):
        supportRdd = self.wlitems.map(lambda item: (item, 1))
        supportRdd = supportRdd.reduceByKey(ut.sumOparator)
        supports = supportRdd.map(lambda item: item[1])

        # Define minimum support value
        if self.minSupport is 'auto':
            minSupport = supports.min()
        else:
            minSupport = self.minSupport

        # If minimum support is 1 then replace it with 2
        minSupport = 2 if minSupport < 2 else minSupport

        # Filter first supportRdd with minimum support
        supportRdd = supportRdd.filter(lambda item: item[1] >= minSupport)

        # Create base RDD with will be updated every iteration
        baseRdd = supportRdd.map(lambda item: ([item[0]], item[1]))

        supportRdd = supportRdd.map(lambda item: item[0])

        c = 2

        while not supportRdd.isEmpty():
            combined = supportRdd.cartesian(self.uniqueItems)
            combined = combined.map(lambda item: ut.removeReplica(item))

            combined = combined.filter(lambda item: len(item) == c)
            combined = combined.distinct()

            combined_2 = combined.cartesian(self.lblitems)
            combined_2 = combined_2.filter(lambda item: all(x in item[1] for x in item[0]))

            combined_2 = combined_2.map(lambda item: item[0])
            combined_2 = combined_2.map(lambda item: (item, 1))
            combined_2 = combined_2.reduceByKey(ut.sumOparator)
            combined_2 = combined_2.filter(lambda item: item[1] >= minSupport)

            baseRdd = baseRdd.union(combined_2)

            combined_2 = combined_2.map(lambda item: item[0])
            supportRdd = combined_2
            c = c + 1

        sets = baseRdd.cartesian(baseRdd)
        filtered = sets.filter(lambda item: ut.filterForConf(item))
        confidences = filtered.map(lambda item: ut.calculateConfidence(item))
        self.confidences = confidences

        return confidences

    def predict(self, set, confidence):

        if not isinstance(set, list):
            raise ValueError('For prediction "set" argument should be a list')

        _confidences = self.confidences
        _filterForPredict = self._filterForPredict

        filtered = _confidences.filter(lambda item: _filterForPredict(item, set, confidence))

        return filtered

    @staticmethod
    def _filterForPredict(item, set, confidence):
        it = item[0]
        it.sort()
        set.sort()
        if it == set and item[2] >= confidence:
            return item
        else:
            pass
