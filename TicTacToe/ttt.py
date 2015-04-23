import sys
import math
import random
# A game of Tic-Tac-Toe
# Assumes that X goes first.

#GenerateMove will return all of the moves in a list
def generateMove(board, width = 3, height = 3):
    dataOnBoard = getNumPieces(board, width * height)
    if dataOnBoard[0] > dataOnBoard[1]:
        pieceToAdd = 'o'
    else:
        pieceToAdd = 'x'
    retList = []
    for i in range(width * height):
        if board[i] == ' ':
            dummyList = []
            for charac in board:
                dummyList.append(charac)
            dummyList[i] = pieceToAdd
            retList.append(dummyList)
    return retList

#Work on getting this to work for square tictactoe of arbitrary size
def isPrimitive(board, width = 3, height = 3):
    dataOnBoard = getNumPieces(board, width, height)
    if dataOnBoard[0] + dataOnBoard[1] == 9:
        return True
    if dataOnBoard[0] < 3:
        return False
    # else:
        # for i in range(width):
        #     columnList =[]
        #     for j in range(height):
        #         columnList.append(board[i + j * width])
        #     if columnList.count(board[i]) == height and board[i] != ' ':
        #         return True

        # for k in range(height):
        #     rowList =[]
        #     for l in range(width):
        #         rowList.append(board[l + k * width])
        #     if rowList.count(board[k]) == width and board[k] != ' ':
        #         return True

        # return False
    else:
        if board[0] != ' ':
            if board[0] == board[1] and board[0] == board[2]:
                return True
            elif board[0] == board[3] and board[3] == board[6]:
                return True
            elif board[0] == board[4] and board[0] == board[8]:
                return True
        
        if board[2] != ' ':
            if board[2] == board[5] and board[2] == board[8]:
                return True
            elif board[2] == board[4] and board[2] == board[6]:
                return True
        
        if board[1] != ' ':
            if board[1] == board[4] and board[1] == board[7]:
                return True
        
        if board[3] != ' ':
            if board[3] == board[4] and board[3] == board[5]:
                return True
        
        if board[6] != ' ':
            if board[6] == board[7] and board[6] == board[8]:
                return True
        return False

def undoMoveList(board, width = 3, height = 3):
    dataOnBoard = getNumPieces(board, width * height)
    if dataOnBoard[0] > dataOnBoard[1]:
        pieceToRemove = 'x'
    else:
        pieceToRemove = 'o'
    retList = []
    for i in range(width * height):
        if board[i] == pieceToRemove:
            dummyList = []
            for charac in board:
                dummyList.append(charac)
            dummyList[i] = ' '
            retList.append(dummyList)
    return retList

#Work on trying to make this work on arbitrary size
def tie(board, width = 3, height = 3):
    if board[0] != ' ':
            if board[0] == board[1] and board[0] == board[2]:
                return 'l'
            elif board[0] == board[3] and board[3] == board[6]:
                return 'l'
            elif board[0] == board[4] and board[0] == board[8]:
                return 'l'
    if board[2] != ' ':
        if board[2] == board[5] and board[2] == board[8]:
            return 'l'
        elif board[2] == board[4] and board[2] == board[6]:
            return 'l'
    
    if board[1] != ' ':
        if board[1] == board[4] and board[1] == board[7]:
            return 'l'
    
    if board[3] != ' ':
        if board[3] == board[4] and board[3] == board[5]:
            return 'l'
    
    if board[6] != ' ':
        if board[6] == board[7] and board[6] == board[8]:
            return 'l'
    return 't'

def isEmpty(board, width = 3, height = 3):
    return [' '] * (width * height) == board


def generateAllMoves(board):
    """
    Generates all possible moves starting from this board and ON until end
    """
    boardsList = []
    boards = generateMove(board)
    boardsList.append(boards)
    if len(boards) > 1:
        boardsList.append([generateAllMoves(b) for b in boards])
    return boardsList

#Think about a way to either have a generic undo move
#or do something clever when it comes to storing children
#on our map downwards


def drawBoard(board):
    """
    Draws board starting from top left to bottom right with index starting at 0
    """
    print('\n' + '===========================' + '\n')
    print('  1   2   3')
    print('    |   |')
    print('1 ' + board[0] + ' | ' + board[1] + ' | ' + board[2])
    print('    |   |')
    print('------------')
    print('    |   |')
    print('2 ' + board[3] + ' | ' + board[4] + ' | ' + board[5])
    print('    |   |')
    print('-----------')
    print('    |   |')
    print('3 ' + board[6] + ' | ' + board[7] + ' | ' + board[8])
    print('    |   |')

def getPlayerLetter(player):
    """
    Returns 'x' if player is 0. Otherwise, 'o'
    """ 
    return 'x' if player == 0 else 'o'

def getNumPieces(board, width = 3, height = 3):
    num_x = board.count('x')
    num_o = board.count('o')
    return (num_x, num_o, (width * height) - num_x - num_o)

def isBoardFull(board, width, height):
    return getNumPieces(board, width, height)[2] == 0

def isValidMove(board, current_player, index, undo=False):
    """
    Move is index number on the board.
    Function to check if some move is valid.
    This includes validation for undoing moves too. Cannot undo a move that is blank on the board.
    """
    num_x, num_o, num_blank = getNumPieces(board)
    if undo: # checking if an undo move is valid.
        # just doublechecking we have correct number of pieces.
        if (current_player == 0) and (num_x - 1 != num_o): # x is undoing.
            return False
        elif (current_player == 1) and (num_x != num_o): # o is undoing.
            return False
        elif num_blank == 9:
            return False
        # checking if the move we are trying to undo is not blank. if it is blank, error!
        if board[index] == ' ':
            return False
    else: # checking if a move made on board is valid
        # doublecheck we have correct number of pieces
        if (current_player == 0) and (num_x != num_o):
            return False
        elif (current_player == 1) and ((num_x - 1) != num_o):
            return False
        elif num_blank == 0:
            return False
        # checking if move we are trying to make is already filled
        if board[index] != ' ':
            return False
    return True

def makeMove(board, current_player, move):
    """
    Makes a move on the board.
    Assumes move has been validated already.
    """
    board[move] = getPlayerLetter(current_player)
    return board

def undoMove(board, move):
    """
    Tic-Tac-Toe undo is just to remove the piece that was just placed.
    Nothing tricky to consider...I think...
    Assumes move has been validated already.
    """
    board[move] = ' '
    return board

def boardStatus(board, current_player):
    """
    board status for the current_player who just made a move
    """
    WIN, UNDECIDED = 0, 1
    board_size = len(board)
    player_letter = getPlayerLetter(current_player)
    p = int(math.sqrt(board_size)) # number of pieces needed in a row to win
    # brute force checking
    # check horizontally
    for i in range (0, board_size, p):
        num_pieces_in_row = 0
        for j in range(i, i+p):
            if board[j] == player_letter:
                num_pieces_in_row += 1
            else:
                num_pieces_in_row = 0
                break
        if num_pieces_in_row == p:
            return WIN
    # check vertically
    for k in range(0, p):
        num_pieces_in_row = 0
        for l in range(k, board_size, p):
            if board[l] == player_letter:
                num_pieces_in_row += 1
            else:
                num_pieces_in_row = 0
                break
        if num_pieces_in_row == p:
            return WIN
    # check diagonal from top left to bottom right
    for m in range(0, board_size, p+1):
        if board[m] == player_letter:
            num_pieces_in_row += 1
        else:
            num_pieces_in_row = 0
        if num_pieces_in_row == p:
            return WIN
    # check diagonal from top right to bottom left
    for n in range(p-1, board_size, p-1):
        if board[n] == player_letter:
            num_pieces_in_row += 1
        else:
            num_pieces_in_row = 0
        if num_pieces_in_row == p:
            return WIN

    return UNDECIDED

def isWin(board, move, current_player):
    """
    Will check if move will be a winning move for current_player
    Perhaps this might be more optimal than checking game status.
    Will do this later.
    """
    return False
    
def initiateBoard(width, height):
    board = [' '] * (width * height)
    return board

def getPlayerMove(n):
    number = input("Enter two-digit number for move ") # first number is row, second number is column. Starts from 1
    row = number / 10
    column = number % 10
    index = (row - 1) * n + (column - 1)
    return index

def run_tests():
    #"Need to write tests...or we can use doctests"
    return True

def main():
    theirInput = raw_input("Type human or computer as your competitor: ")
    args = ["human", theirInput]
    # args = sys.argv[1:] # just want the arguments after python ttt.py
    assert(len(args)) == 2
    assert run_tests() == True
    # allowed modes:
    # human vs human
    # human vs computer
    # computer vs human

    #Testing to see if generateMove worked. It did with no
    #entires, with entry x, and with entries x and o

    boardTest = initiateBoard(3, 3)
    boardTest[0] = 'x'
    boardTest[3] = 'x'
    boardTest[6] = 'x'
    boardTest[8] = 'o'
    boardTest[7] = 'o'
    listOfPossMoves = undoMoveList(boardTest, 3, 3)
    print(listOfPossMoves)


    first_player = args[0]
    second_player = args[1]
    n = 3
    board = initiateBoard(n, n)
    game_is_active = True
    need_move = False
    current_player = 0 # toggle player
    if first_player == 'human':
        if second_player == 'computer':
            while game_is_active:
                if current_player == 0: # x's turn
                    drawBoard(board)
                    need_move = not need_move
                    while need_move:
                        move = getPlayerMove(n)
                        if isValidMove(board, current_player, move):
                            makeMove(board, current_player, move)
                            need_move = not need_move
                        else:
                            print("Invalid move. Enter again.")
                    if boardStatus(board, current_player) == 0:
                        print("You just won!")
                        game_is_active = not game_is_active
                        drawBoard(board)
                        break
                    elif isBoardFull(board):
                        print("You have tied!")
                        game_is_active = not game_is_active
                        drawBoard(board)
                        break
                    current_player = 1 - current_player
                elif current_player == 1:
                    need_move = not need_move
                    while need_move:
                        move = random.randint(0, n*n-1)
                        if isValidMove(board, current_player, move):
                            makeMove(board, current_player, move)
                            need_move = not need_move
                    if boardStatus(board, current_player) == 0:
                        print("You just lost!")
                        game_is_active = not game_is_active
                        drawBoard(board)
                        break
                    elif isBoardFull(board):
                        print("You have tied!")
                        game_is_active = not game_is_active
                        drawBoard(board)
                        break
                    current_player = 1 - current_player
                drawBoard(board)


if __name__ == "__main__":
    main()
