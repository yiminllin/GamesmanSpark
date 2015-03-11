import gamesman as G
params = G.params()

width = params.setdefault('width', 7)
height = params.setdefault('height', 6)
win = params.setdefault('win', 4)

def get_columns(position):
    return [position[i:i+height] for i in range(0, width * height, height)]

def turn(position):
    # Default to 'o''s turn ('o' goes first).
    if position.count('o') > position.count('x'):
        return 'x'
    else:
        return 'o'

def to_bits(column):
    column = column.strip()
    out = 0
    out |= 0x1 << len(column)
    for i, c in enumerate(column):
        if c == 'x':
            out |= 0x1 << len(column) - i - 1
    return out

def hash_brd(brd, width, height):
    columns = get_columns(brd, width, height)
    bits_per_column = height + 1
    out = 0
    for i, col in enumerate(columns):
        out |= to_bits(col) << (i * bits_per_column)
    return out

def index_of_last(string, char):
    return len(string) - string[::-1].find(char) - 1

def move_column(turn, column):
    last_space = index_of_last(column, ' ')
    out = column[:last_space] + turn + column[last_space+1:]
    return out

def get_rows(position):
    return [position[i::width] for i in range(0, height)]


def get_p_diags(position):
    return [position[i::width + 1] for i in range(0, height)]


def get_n_diags(position):
    return [position[i::width - 1] for i in range(0, height)]

def get_diagonals(position):
    return get_p_diags(position) + get_n_diags(position)

def winner(position):
    winx = 'x' * win
    wino = 'o' * win
    cols = get_columns(position)
    rows = get_rows(position)
    diags = get_diagonals(position)
    for c in cols + rows + diags:
        if winx in c:
            return 'x'
        elif wino in c:
            return 'o'

## GamesmanSpark API is implemented below.

initialPosition = ' ' * width * height

def generateMoves(position):
    columns = get_columns(position)
    return [i for i in range(width) if ' ' in columns[i]]

def doMove(position, move):
    columns = get_columns(position)
    new_col = move_column(turn(position), columns[move])
    return ''.join(columns[:move] + [new_col] + columns[move + 1:])

def primitive(position):
    player = turn(position)
    w = winner(position)
    if w is not None:
        if player != w:
            return G.WIN
        else:
            return G.LOSE
    else:
        if ' ' not in position:
            return G.TIE
        else:
            return G.UNDECIDED
