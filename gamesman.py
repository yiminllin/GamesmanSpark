import imp as _imp
import argparse as _argparse
import os as _os
import re as _re

WIN, LOSE, TIE, UNDECIDED, DRAW = 'win', 'lose', 'tie', 'undecided', 'draw'

DRAW_REMOTENESS = float('inf')

PARAMS = {}

def params():
    return PARAMS

def load_game_from_args(description):
    global PARAMS
    parser = _argparse.ArgumentParser(description=description)
    parser.add_argument('game', help='The path to the game script to run.')
    arg, extra = parser.parse_known_args()
    extra_re = _re.compile('--([^= ]+)=([^ ]+)')
    arg_pairs = [extra_re.match(e).groups() for e in extra]
    for i, pair in enumerate(arg_pairs):
        try:
            value = eval(pair[1])
            arg_pairs[i] = (pair[0], value)
        except ValueError:
            pass
    game_params = dict(arg_pairs)
    PARAMS = game_params
    name = _os.path.split(_os.path.splitext(arg.game)[0])[-1]
    game = _imp.load_source(name, arg.game)
    filled_params = sorted(PARAMS.items())
    full_name = name
    if filled_params:
        argstring = ''.join(['{}={}'.format(k, v) for k, v in filled_params])
        full_name = '{}-{}'.format(name, argstring)
    return full_name, game

