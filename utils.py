# Sum oparation for reduce by key
def sumOparator(x, y):
    return x + y


# Remove replications after cartesian oparation
def removeReplica(record):
    if (isinstance(record[0], tuple)):
        x1 = record[0]
        x2 = record[1]
    else:
        x1 = [record[0]]
        x2 = record[1]

    if (any(x == x2 for x in x1) == False):
        a = list(x1)
        a.append(x2)
        a.sort()
        result = tuple(a)
        return result
    else:
        return x1


# Filter items
def filterForConf(item):
    if len(item[0][0]) > len(item[1][0]):
        if not checkItemSets(item[0][0], item[1][0]):
            pass
        else:
            return item
    else:
        pass


# Check Items sets includes at least one comman item // Example command: # any(l == k for k in z for l in x )
def checkItemSets(item_1, item_2):
    if len(item_1) > len(item_2):
        return all(any(k == l for k in item_1) for l in item_2)
    else:
        return all(any(k == l for k in item_2) for l in item_1)


# Confidence calculation
def calculateConfidence(item):
    # Parent item list
    parent = set(item[0][0])

    # Child item list
    if isinstance(item[1][0], str):
        child = set([item[1][0]])
    else:
        child = set(item[1][0])
    # Parent and Child support values
    parentSupport = item[0][1]
    childSupport = item[1][1]
    # Finds the item set confidence is going to be found

    support = (parentSupport / childSupport) * 100

    return list([list(child), list(parent.difference(child)), support])
