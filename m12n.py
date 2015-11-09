import gamesman as G
params = G.params()

end = params.setdefault('end', 4)

initialPosition = 1

def primitive(position):
    if position == end:
        return G.LOSE
    else:
        return G.UNDECIDED

def generateMoves(position):
    if position + 2 > end:
        return [1]
    else:
        return [1, 2]

def doMove(position, move):
    return position + move
