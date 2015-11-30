import gamesman

def main():
    name, game = gamesman.load_game_from_args('Naively solve games.')
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
