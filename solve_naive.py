import imp
import argparse
import os
import gamesman

def main():
    parser = argparse.ArgumentParser(description='Naively solves games')
    parser.add_argument('game', help='The path to the game script to run.')
    arg = parser.parse_args()
    name = os.path.split(os.path.splitext(arg.game)[0])[-1]
    game = imp.load_source(name, arg.game)
    print(solve(game, game.initialPosition))

def solve(game, pos):
    v = game.primitive(pos)
    if v is not gamesman.UNDECIDED:
        return v
    else:
        moves = game.generateMoves(pos)
        vals = [solve(game, game.doMove(pos, m)) for m in moves]
        if gamesman.LOSE in vals:
            return gamesman.WIN
        elif gamesman.TIE in vals:
            return gamesman.TIE
        else:
            return gamesman.LOSE

if __name__ == '__main__':
    main()
