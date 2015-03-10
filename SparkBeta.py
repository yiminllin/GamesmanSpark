from pyspark import SparkContext
from ttt import initiateBoard, generateMove, isPrimitive

n = 3

def bfs_map(value):
    retVal = []
    if value[1][0] == boardLevel and not isPrimitive(value[0]):
        children = generateMove(value[0], n)

        for child in children:
            parentTuple = [boardLevel + 1, tuple( [tuple(value[0])] )]
            childTuple = [tuple(child), tuple(parentTuple)]

            retVal.append(tuple(childTuple))

    return retVal

def bfs_reduce(value1, value2):
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

def myPrim(value):
    return isPrimitive(value[0])

def printFunction(rdd, fName):
    #Running through the list and formatting it as level and then board set
    outputFile = open(fName, "w")
    writer = lambda line: outputFile.write(str(line) + "\n")
    compiledArr = rdd.collect()

    for elem in compiledArr:
        writer(elem)

def main():
    global boardLevel
    boardLevel = 0
    sc = SparkContext("local[1]", "python")

    blankBoard = initiateBoard(n)

    rdd = [(tuple(blankBoard), (boardLevel, ()))]
    rdd = sc.parallelize(rdd)


    for _ in range(9):
        finishedLevel = rdd.flatMap(bfs_map)
        done = finishedLevel.reduceByKey(bfs_reduce)
        boardLevel += 1
        rdd += done

    allPrimRDD = rdd.filter(myPrim)
    
    # #printFunction(finishedLevel, "test.txt")
    # #Figure out why this one crashes
    printFunction(rdd, "output.txt")
    printFunction(allPrimRDD, "primitives.txt")



if __name__ == "__main__":
    main()
