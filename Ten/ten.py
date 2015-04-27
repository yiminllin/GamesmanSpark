import sys
import math
import random

def initiateBoard(start, end):
    return [start]

def generateMove(pos, start, end):
    currentNumber = pos[0]

    if (currentNumber + 1 == end):
        return [[currentNumber + 1]]
    
    return [[currentNumber + 1], [currentNumber + 2]]

def isPrimitive(pos, start, end):
    return pos[0] == end

def tie(pos, start, end):
    return 'l'

def isEmpty(pos, start, end):
    return pos[0] == start