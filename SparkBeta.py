from pyspark import SparkContext
from TicTacToe.ttt import initiateBoard, generateMove, isPrimitive, winner

n = 3

"""
Assigning all the wins/loses
"""
def traceBackUpMap(value):
    return value

"""
Figure out the win/lose idea
"""
def primitiveWinOrLoseMap(value):
    return tuple([(value[0], winner(value[0]))])

"""
Getting all the primitives
"""
def bfsMap(value):
    retVal = [value]
    if value[1][0] == boardLevel and not isPrimitive(value[0]):
        children = generateMove(value[0], n)

        for child in children:
            parentTuple = [boardLevel + 1, tuple( [tuple(value[0])] )]
            childTuple = [tuple(child), tuple(parentTuple)]

            retVal.append(tuple(childTuple))
    return retVal

def bfsReduce(value1, value2):
    if value1[0] <= value2[0]:
        allParents = []
        for eachParent in value1[1]:
            allParents.append(tuple(eachParent))
        for eachParent in value2[1]:
            allParents.append(tuple(eachParent))
        return (value1[0], tuple(allParents))
    else:
        allParents = []
        for eachParent in value1[1]:
            allParents.append(tuple(eachParent))
        for eachParent in value2[1]:
            allParents.append(tuple(eachParent))
        return (value2[0], tuple(allParents))

def filteringPrimitives(value):
    return isPrimitive(value[0])

def printBFSFunction(rdd, fName):
    #Running through the list and formatting it as level and then board set
    outputFile = open(fName, "w")
    writer = lambda line: outputFile.write(str(line) + "\n")
    compiledArr = rdd.collect()
    compiledArr = sorted(compiledArr, key = lambda value: value[1][0])

    for elem in compiledArr:
        writer(elem)

def printTraceBackFunction(rdd, fName):
    outputFile = open(fName, "w")
    writer = lambda line: outputFile.write(str(line) + "\n")
    compiledArr = rdd.collect()
    compiledArr = sorted(compiledArr, key = lambda value: value[1])

    for elem in compiledArr:
        writer(elem)

def relevantSet(value):
    return value[1][0] == boardLevel

def main():
    global boardLevel
    boardLevel = 0
    sc = SparkContext("local[1]", "python")

    blankBoard = initiateBoard(n)

    rdd = [(tuple(blankBoard), (boardLevel, ()))]
    rdd = sc.parallelize(rdd)

    num = 1

    while num:
        rdd = rdd.flatMap(bfsMap)
        rdd = rdd.reduceByKey(bfsReduce)
        num = rdd.filter(relevantSet).count()
        boardLevel += 1

    allPrimRDD = rdd.filter(filteringPrimitives)
    
    # #printBFSFunction(finishedLevel, "test.txt")
    # #Figure out why this one crashes
    printBFSFunction(rdd, "Results/output.txt")
    printBFSFunction(allPrimRDD, "Results/primitives.txt")

    testing = allPrimRDD.flatMap(primitiveWinOrLoseMap)
    printTraceBackFunction(testing, "Results/testing.txt")



if __name__ == "__main__":
    main()
