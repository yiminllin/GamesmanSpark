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
value = (boardState, (w/t/l, remoteValue, (Parents)))
"""
def traceBackUpMap(value):
    retVal = []
    #If we are not at the correct remoteness level, or we have one
    #of the conflicted mapping, move past and just append retval with
    #current value for next go around
    if type(value[1][1]) is int and remoteness == value[1][1]:
        if not currMod.isEmpty(value[0], width, height):
            parents = value[1][2]

            retVal.append(value)

            for parent in parents:
                boardInformation = [opposite(value[1][0]), remoteness + 1, ()]
                parentTuple = [parent, tuple(boardInformation)]

                retVal.append(tuple(parentTuple))
    else:
        retVal.append(value)

    return retVal

"""
Possible merging of our values going down and values coming up
Conflict between:
    possibleValue1: (w/t/l, remoteValue, (Parents))
    possibleValue2: (depthAway, (Children))
"""

def traceBackUpReduce(value1, value2):
    #Checks to see if one of the values is the information
    #from our mapping down
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
    #This is when we are only dealing with both values being on the map
    #back up
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

        gamePosition2 = value2[0]
        gamePosition1 = value1[0]

        remoteness1 = value1[1]
        remoteness2 = value2[1]

        if gamePosition2 == 'w':
            if gamePosition1 == 'w':
                if remoteness1 > remoteness2:
                    remote = remoteness2
                else:
                    remote = remoteness1
            else:
                remote = remoteness2
            boardState = 'w'
        elif gamePosition1 == 'w':
            boardState = 'w'
            remote = remoteness1
        elif gamePosition2 == 't':
            if gamePosition1 == 't':
                if remoteness1 > remoteness2:
                    remote = remoteness2
                else:
                    remote = remoteness1
            else:
                remote = remoteness2
            boardState = 't'
        elif gamePosition1 == 't':
            boardState = 't'
            remote = remoteness1
        else:
            boardState = 'l'
            if remoteness1 > remoteness2:
                remote = remoteness1
            else:
                remote = remoteness2

        tempTuple = (boardState, remote, tuple(parentLst))
        return tempTuple

"""
Figure out the win/lose idea
"""
def primitiveWinOrLoseMap(value):
    return tuple([(value[0], tuple([winner(value[0]), value[1][1]]))])

"""
Getting all the primitives
value = (boardState, (depthAway, (children)))
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
    printBFSFunction(rdd, "Results/" + sys.argv[4] + "FirstMappingOutput.txt")

    #Output of all the primitives
    printBFSFunction(allPrimRDD, "Results/" + sys.argv[4] + "Primitives.txt")

    num = 1

    while num:
        rdd = rdd.flatMap(traceBackUpMap)
        rdd = rdd.reduceByKey(traceBackUpReduce)
        num = rdd.filter(relevantRemote).collect()
        remoteness += 1

    printTraceBackFunction(rdd, "Results/" + sys.argv[4] + "FinalMappingOutput.txt")



if __name__ == "__main__":
    main()
