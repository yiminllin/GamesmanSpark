import sys
import math
import random

def initiateBoard(width, height):
    return [0]

def generateMove(pos, width, height):
    currentNumber = pos[0]

    if (currentNumber + 1 == 10):
        return [[currentNumber + 1]]
    
    return [[currentNumber + 1], [currentNumber + 2]]

def isPrimitive(pos, width, height):
    return pos[0] == 10

def tie(pos, width, height):
    return 'l'

def isEmpty(pos, width, height):
    return pos[0] == 0