import gamesman as G

initialPosition = 0

def primitive(position):
    if position == 10:
        return G.LOSE
    else:
        return G.UNDECIDED

def generateMoves(position):
    if position == 9:
        return [1]
    else:
        return [1, 2]

def doMove(position, move):
    return position + move
