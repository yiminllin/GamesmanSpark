from pyspark import SparkContext
#from TicTacToe.ttt import initiateBoard, generateMove, isPrimitive, tie, isEmpty
import sys
import importlib

"""
Needed:
	-initiateBoard(width, height)
	-generateMove(pos, width, height)
	-isPrimitive(pos, width, height)
	-tie(pos, width, height)
	-isEmpty(pos, width, height)
"""


def opposite(letter):
	if letter == 'w':
		return 'l'
	if letter == 'l':
		return 'w'
	return letter


"""
Assigning all the wins/loses
value = (boardState, (boardLevel, w/t/l, (Parents), remoteness))
"""
def traceBackUpMapNew(value):
	retVal = []
	#If we are not at the correct remoteness level, or we have one
	#of the conflicted mapping, move past and just append retval with
	#current value for next go around
	if type(value[1][1]) is str and boardLevel == value[1][0]:
		if not currMod.isEmpty(value[0], width, height):
			parents = value[1][2]

			retVal.append(value)

			for parent in parents:
				#We flip the game state and choose our parent's state based
				#on these values
				if not currMod.isPrimitive(parent, width, height):
					boardInformation = [boardLevel - 1, opposite(value[1][1]), (), value[1][3] + 1]
					parentTuple = [parent, tuple(boardInformation)]
					retVal.append(tuple(parentTuple))
	else:
		retVal.append(value)

	return retVal

"""
Possible merging of our values going down and values coming up
Conflict between:
	possibleValue1: (boardLevel, w/t/l, (Parents), remoteness)
	possibleValue2: (boardLevel, (Children))
"""

def traceBackUpReduceNew(value1, value2):
	#Checks to see if one of the values is the information
	#from our mapping down
	if type(value1[1]) is tuple and type(value2[1]) is str:
		parentLst = []
		value1List = value1[1]
		for val in value1List:
			parentLst.append(val)
		value2List = value2[2]
		for val in value2List:
			parentLst.append(val)
		tempTuple = (value1[0], value2[1], tuple(parentLst), value2[3])
		return tempTuple
	elif type(value2[1]) is tuple and type(value1[1]) is str:
		parentLst = []
		value2List = value2[1]
		for val in value2List:
			parentLst.append(val)
		value1List = value1[2]
		for val in value1List:
			parentLst.append(val)
		tempTuple = (value2[0], value1[1], tuple(parentLst), value1[3])
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
		remote = -1

		gamePosition2 = value2[1]
		gamePosition1 = value1[1]

		remoteness1 = value1[3]
		remoteness2 = value2[3]

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

		tempTuple = (max(value1[0], value2[0]), boardState, tuple(parentLst), remote)
		return tempTuple

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
				#We flip the game state and choose our parent's state based
				#on these values
				if not currMod.isPrimitive(parent, width, height):
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
		remote = -1

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

def beautyMap(value):
	return tuple([value[0][1], tuple([value[1][0], value[1][1], value[1][3], value[1][2]])])
def pointlessReduce(value1, value2):
	return value1

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
	if type(value1[0]) is int and type(value2[0]) is int:
		if value1[0] <= value2[0] :
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
	else:
		if type(value1[0]) is int:
			allParents = []
			for eachParent in value1[1]:
				allParents.append(tuple(eachParent))
			for eachParent in value2[2]:
				allParents.append(tuple(eachParent))
			return (value2[0], value2[1], tuple(allParents))
		elif type(value2[0]) is int:
			allParents = []
			for eachParent in value2[1]:
				allParents.append(tuple(eachParent))
			for eachParent in value1[2]:
				allParents.append(tuple(eachParent))
			return (value1[0], value1[1], tuple(allParents))
		else:
			return ("blank", -1, ())

"""
Getting all the primitives
value = (boardState, (depthAway, (children)))
"""
def bfsMapNew(value):
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
			tempTuple = tuple([value[0], tuple([value[1][0], currMod.tie(value[0], width, height), value[1][1], 0])])
			retVal.append(tempTuple)
	else:
		retVal.append(value)
	return retVal

"""
(depthAway, (children))
Want the values to be (depthAway, gameState, (children), remoteness)
"""
def bfsReduceNew(value1, value2):
	if type(value1[1]) is tuple and type(value2[1]) is tuple:
		if value1[0] <= value2[0] :
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
	else:
		if type(value1[1]) is tuple:
			allParents = []
			for eachParent in value1[1]:
				allParents.append(tuple(eachParent))
			for eachParent in value2[2]:
				allParents.append(tuple(eachParent))
			return (value2[0], value2[1], tuple(allParents), value2[3])
		elif type(value2[0]) is tuple:
			allParents = []
			for eachParent in value2[1]:
				allParents.append(tuple(eachParent))
			for eachParent in value1[2]:
				allParents.append(tuple(eachParent))
			return (value1[0], value1[1], tuple(allParents), value1[3])
		else:
			return ("blank", -1, ())


"""
Assigning all the wins/loses
value = (boardState, (w/t/l, remoteValue))
"""
def traceBackUpMapList(value):
	retVal = []
	#If we are not at the correct remoteness level, or we have one
	#of the conflicted mapping, move past and just append retval with
	#current value for next go around
	if remoteness == value[1][1]:
		if not currMod.isEmpty(value[0], width, height):
			parents = currMod.undoMoveList(value[0], width, height)

			retVal.append(value)

			for parent in parents:
				#We flip the game state and choose our parent's state based
				#on these values
				if not currMod.isPrimitive(parent, width, height):
					boardInformation = [opposite(value[1][0]), remoteness + 1, ]
					parentTuple = [tuple(parent), tuple(boardInformation)]

					retVal.append(tuple(parentTuple))
	else:
		retVal.append(value)

	return retVal

"""
Possible merging of our values going down and values coming up
Conflict between:
	possibleValue1: (w/t/l, remoteValue)
"""

def traceBackUpReduceList(value1, value2):
	#Checks to see if one of the values is the information
	#from our mapping down
	# if remoteness == value1[1] and remoteness == value2[1]:
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
			remote = remoteness2
		else:
			remote = remoteness1

	tempTuple = (boardState, remote)
	return tempTuple
	# elif remoteness == value1[1]:
	# 	return value1
	# elif remoteness == value2[1]:
	# 	return value2
	# else:
	# 	#raise Exception("I missed something\n" + str(value1) + "\n" + str(value2))
	# 	return ("", -1)


"""
value = (board state, boardlevel)
"""
def bfsMapList(value):
	retVal = []
	if value[1] == boardLevel:
		if not currMod.isPrimitive(value[0], width, height):
			children = currMod.generateMove(value[0], width, height)

			for child in children:
				childTuple = [tuple(child), boardLevel + 1]
				retVal.append(tuple(childTuple))
		else:
			tempTuple = tuple([value[0], tuple([currMod.tie(value[0], width, height), 0])])
			retVal.append(tempTuple)
	elif currMod.isPrimitive(value[0]):
		retVal.append(value)
	return retVal

def bfsReduceList(value1, value2):
	return min(value1, value2)


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

def printBFSListFunction(rdd, fName):
	#Running through the list and formatting it as level and then board set
	outputFile = open(fName, "w")
	writer = lambda line: outputFile.write(str(line) + "\n")
	compiledArr = rdd.collect()
	compiledArr = sorted(compiledArr, key = lambda value: value[1])

	for elem in compiledArr:
		writer(elem)

def printTraceBackFunction(rdd, fName):
	outputFile = open(fName, "w")
	writer = lambda line: outputFile.write(str(line) + "\n")
	compiledArr = rdd.collect()
	compiledArr = sorted(compiledArr, key = lambda value: value[1][1])

	for elem in compiledArr:
		writer(elem)

def printTraceBackFunctionNew(rdd, fName):
	outputFile = open(fName, "w")
	writer = lambda line: outputFile.write(str(line) + "\n")
	compiledArr = rdd.collect()
	compiledArr = sorted(compiledArr, key = lambda value: value[0])

	for elem in compiledArr:
		writer(elem)

def relevantSet(value):
	return value[1][0] == boardLevel

def relevantSetList(value):
	return value[1] == boardLevel

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
	maxBoardLevel = 0

	while num:
		maxBoardLevel = boardLevel
		rdd = rdd.flatMap(bfsMapNew)
		rdd = rdd.reduceByKey(bfsReduceNew)
		num = rdd.filter(relevantSet).count()
		boardLevel += 1
	
	#Output after BFS downwards
	printBFSFunction(rdd, "Results/" + sys.argv[4] + "FirstMappingOutput.txt")

	allPrimRDD = rdd.filter(filteringPrimitives)

	#Output of all the primitives
	printBFSFunction(allPrimRDD, "Results/" + sys.argv[4] + "Primitives.txt")

	# remoteness = 0
	# blankBoard = currMod.initiateBoard(width, height)

	# boardLevel = 0
	# testRDD = [(tuple(blankBoard), boardLevel)]
	# testRDD = sc.parallelize(testRDD)
	# num = 1

	# while num:
	# 	testRDD = testRDD.flatMap(bfsMapList)
	# 	testRDD = testRDD.reduceByKey(bfsReduceList)
	# 	boardLevel += 1
	# 	num = testRDD.filter(relevantSetList).count()
	# printBFSListFunction(testRDD, "Results/" + sys.argv[4] + "Primitives2.txt")

	# num = 1
	# while num:
	# 	testRDD = testRDD.flatMap(traceBackUpMapList)
	# 	testRDD = testRDD.reduceByKey(traceBackUpReduceList)
	# 	remoteness += 1
	# 	num = testRDD.filter(relevantRemote).count()
	# 	printBFSListFunction(testRDD, "Results/" + sys.argv[4] + "Testing" + str(remoteness) + ".txt")

	num = 1
	i = 0
	boardLevel = maxBoardLevel - 1
	while num:
		rdd = rdd.flatMap(traceBackUpMapNew)
		printTraceBackFunction(rdd, "Results/BoardLevel" + str(boardLevel) + ".txt")
		printTraceBackFunction(rdd, "Results/" + sys.argv[4] + "Before" + str(i) + ".txt")
		rdd = rdd.reduceByKey(traceBackUpReduceNew)
		printTraceBackFunction(rdd, "Results/" + sys.argv[4] + "After" + str(i) + ".txt")
		num = rdd.filter(relevantSet).collect()
		boardLevel -= 1
		i += 1
	beautifiedRDD = rdd.flatMap(beautyMap)
	printTraceBackFunctionNew(beautifiedRDD, "Results/" + sys.argv[4] + "FinalMappingOutput.txt")

def flipRDD(value):
	retval = [value[1], value[0]]
	return tuple(retval)



if __name__ == "__main__":
	main()
