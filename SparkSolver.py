from pyspark import SparkContext
#from TicTacToe.ttt import initiateBoard, generateMove, isPrimitive, tie, isEmpty
import sys
import importlib

"""
Needed:
    -initiateBoard
    -generateMove
    -isPrimitive
    -tie
    -isEmpty
"""


def opposite(letter):
    if letter == 'w':
        return 'l'
    if letter == 'l':
        return 'w'
    return letter


"""
Assigning all the wins/loses
"""
def traceBackUpMap(value):
    retVal = []
    if remoteness != 0:
        if remoteness == value[1][1]:
            if not currMod.isEmpty(value[0], width, height):
                parents = value[1][2]

                retVal.append(value)

                for parent in parents:
                    boardInformation = [opposite(value[1][0]), remoteness + 1, ()]
                    parentTuple = [parent, tuple(boardInformation)]

                    retVal.append(tuple(parentTuple))
        else:
            retVal.append(value)
    else:
        if remoteness == value[1][1]:
            if not currMod.isEmpty(value[0], width, height):
                parents = value[1][2]

                tempTuple = [value[0], []]
                retVal.append(value)

                for parent in parents:
                    boardInformation = [opposite(value[1][0]), remoteness + 1, ()]
                    parentTuple = [parent, tuple(boardInformation)]

                    retVal.append(tuple(parentTuple))

        else:
            retVal.append(value)

    return retVal

def traceBackUpReduce(value1, value2):
    if type(value1[0]) is int and type(value2[0]) is str:
        parentLst = []
        value1List = value1[1]
        for val in value1List:
            parentLst.append(val)
        value2List = value2[2]
        for val in value2List:
            parentLst.append(val)
        tempTuple = (value2[0], value2[1], tuple(parentLst))
        return tempTuple
    elif type(value2[0]) is int and type(value1[0]) is str:
        parentLst = []
        value2List = value2[1]
        for val in value2List:
            parentLst.append(val)
        value1List = value1[2]
        for val in value1List:
            parentLst.append(val)
        tempTuple = (value1[0], value1[1], tuple(parentLst))
        return tempTuple
    else:
        parentLst = []
        value1List = value1[2]
        for val in value1List:
            parentLst.append(val)
        value2List = value2[2]
        for val in value2List:
            parentLst.append(val)

        boardState = ''
        remote = 1000

        if value2[0] == 'w':
            if value1[0] == 'w':
                if value1[1] > value2[1]:
                    remote = value2[1]
                else:
                    remote = value1[1]
            boardState = 'w'
        elif value1[0] == 'w':
            boardState = 'w'
            remote = value1[1]

        elif value2[0] == 't':
            if value1[0] == 't':
                if value1[1] > value2[1]:
                    remote = value1[1]
                else:
                    remote = value2[1]
            boardState = 't'
        elif value1[0] == 't':
            boardState = 't'
            remote = value1[1]
        else:
            boardState = 'l'
            if value1[1] > value2[1]:
                remote = value1[1]
            else:
                remote = value2[1]

        tempTuple = (boardState, remote, tuple(parentLst))
        return tempTuple

"""
Figure out the win/lose idea
"""
def primitiveWinOrLoseMap(value):
    return tuple([(value[0], tuple([winner(value[0]), value[1][1]]))])

"""
Getting all the primitives
"""
def bfsMap(value):
    retVal = []
    if value[1][0] == boardLevel:
        if not currMod.isPrimitive(value[0], width, height):
            children = currMod.generateMove(value[0], width, height)

            for child in children:
                parentTuple = [boardLevel + 1, tuple( [tuple(value[0])] )]
                childTuple = [tuple(child), tuple(parentTuple)]

                retVal.append(tuple(childTuple))
            retVal.append(value)
        else:
            tempTuple = tuple([value[0], tuple([currMod.tie(value[0], width, height), 0, value[1][1]])])
            retVal.append(tempTuple)
    else:
        retVal.append(value)
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
    return currMod.isPrimitive(value[0], width, height)

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
    compiledArr = sorted(compiledArr, key = lambda value: value[1][1])

    for elem in compiledArr:
        writer(elem)

def relevantSet(value):
    return value[1][0] == boardLevel

def relevantRemote(value):
    return value[1][1] == remoteness

def main():
    #Current board level (tier) in our bfs
    global boardLevel
    boardLevel = 0

    #Converting command-line argument into an int
    global width, height
    width = int(sys.argv[1])
    height = int(sys.argv[2])

    #Module of game we are playing
    global currMod
    currMod = importlib.import_module(sys.argv[3], package=sys.argv[4])

    #Remoteness to win, lose, tie, draw (Using as a tier to traverse up the list)
    global remoteness
    remoteness = 0

    sc = SparkContext("local[1]", "python")
    
    blankBoard = currMod.initiateBoard(width, height)

    rdd = [(tuple(blankBoard), (boardLevel, ()))]
    rdd = sc.parallelize(rdd)

    num = 1

    while num:
        rdd = rdd.flatMap(bfsMap)
        rdd = rdd.reduceByKey(bfsReduce)
        num = rdd.filter(relevantSet).count()
        boardLevel += 1

    allPrimRDD = rdd.filter(filteringPrimitives)
    
    #Output after BFS downwards
    printBFSFunction(rdd, "Results/" + sys.argv[4] + "Output.txt")

    #Output of all the primitives
    printBFSFunction(allPrimRDD, "Results/" + sys.argv[4] + "Primitives.txt")

    num = 1

    while num:
        rdd = rdd.flatMap(traceBackUpMap)
        rdd = rdd.reduceByKey(traceBackUpReduce)
        num = rdd.filter(relevantRemote).collect()
        remoteness += 1

    printTraceBackFunction(rdd, "Results/" + sys.argv[4] + "Checking.txt")



if __name__ == "__main__":
    main()
