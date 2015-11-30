from __future__ import print_function
import imp
import argparse
import os
import gamesman
from mpi4py import MPI
import functools
import time
from collections import defaultdict
import os
import hashlib


COMM_WORLD = MPI.COMM_WORLD
COMM_RANK = COMM_WORLD.Get_rank()
COMM_SIZE = COMM_WORLD.Get_size()
TAG_KEYREDUCER = 1
PENDING_CHECK_INTERVAL = 0.5
NUMBER_SIMULTANEOUS_REQUESTS = 64

def consistent_hash(obj):
    m = hashlib.sha1()
    m.update(repr(obj).encode())
    result = int.from_bytes(m.digest(), 'little')
    return result

class KeyReducer(object):

    class Router(object):

        def __init__(self):
            self.names_to_reducer = dict()
            self._incoming_len = NUMBER_SIMULTANEOUS_REQUESTS
            self._incoming = []
            for i in range(self._incoming_len):
                self._incoming.append(COMM_WORLD.irecv(tag=TAG_KEYREDUCER))

        def recv(self, data):
            rank, name, key, value = data
            reducer = self.names_to_reducer[name]
            assert rank == COMM_RANK
            assert reducer.is_local(key)
            reducer.send(key, value)

        def process_recv(self):
            active = False
            for i, req in enumerate(self._incoming):
                ready, data = req.test()
                if ready:
                    active = True
                    self.recv(data)
                    self._incoming[i] = COMM_WORLD.irecv(tag=TAG_KEYREDUCER)
            if not active:
                time.sleep(0)

        def any_pending(self):
            for name, reducer in sorted(self.names_to_reducer.items()):
                pending = COMM_WORLD.reduce(reducer._pending)
                done = pending == 0
                if COMM_RANK == 0:
                    COMM_WORLD.bcast(done)
                    return done
                else:
                    res = COMM_WORLD.bcast(None)
                    return res

        def print_all(self):
            for name, reducer in self.names_to_reducer.items():
                print(name, reducer.local)

        def run_until_done(self):
            reducer_incoming = dict()
            last_pending_check = time.time()
            while True:
                self.process_recv()
                if last_pending_check + PENDING_CHECK_INTERVAL < time.time():
                    last_pending_check = time.time()
                    if not self.any_pending():
                        break

    router = Router()

    def __init__(self, name):
        self.name = name
        self.rank = COMM_RANK
        self.world_size = COMM_SIZE
        self.local = dict()
        self.router.names_to_reducer[name] = self
        self._pending = 0
        self._reqs = []

    def compute_rank(self, key):
        return consistent_hash(key) % self.world_size

    def is_local(self, key):
        return self.compute_rank(key) == self.rank

    def send(self, key, val):
        if self.is_local(key):
            self.local[key] = self.reduce(key, self.local.get(key, None), val)
            self._pending -= 1
        else:
            self._pending += 1
            dest = self.compute_rank(key)
            req = COMM_WORLD.isend((dest, self.name, key, val),
                                   dest=dest,
                                   tag=TAG_KEYREDUCER)
            # We should be able to release req here, but mpi4py causes memory
            # corruption if we do. We need to hang onto a reference to req
            # until the request is done.
            self._reqs.append(req)
            if len(self._reqs) > NUMBER_SIMULTANEOUS_REQUESTS:
                self._reqs = [r for r in self._reqs if not r.test()[0]]

    def reduce(self, key, old_val, new_val):
        raise NotImplemented("reduce")

    def foreach(self, func):
        for key, val in self.local.items():
            func(key, val)

    def save(self, dirname):
        if COMM_RANK == 0:
            os.makedirs(dirname)
        COMM_WORLD.barrier()
        with open(os.path.join(dirname, 'part-{:05}'.format(COMM_RANK)), 'w') as out:
            for key, value in self.local.items():
                out.write('{!r} {!r}\n'.format(key, value))
        COMM_WORLD.barrier()
        if COMM_RANK == 0:
            with open(os.path.join(dirname, '_SUCCESS'), 'w') as out:
                pass

def keyReduce(func):
    OPTIONAL = object()

    class WrapperKeyReducer(KeyReducer):

        def __init__(self):
            KeyReducer.__init__(self, 'keyReduce({})'.format(func.__name__))

        def reduce(self, key, old_val, new_val):
            return func(key, old_val, new_val)

        def __call__(self, key, val=OPTIONAL):
            if val is OPTIONAL:
                return self[key]
            else:
                self.send(key, val)

    return WrapperKeyReducer()

class ChildValAccumulator(object):

    first = lambda a, b: a
    second = lambda a, b: b
    REDUCTION_TABLE = {
        (gamesman.LOSE , gamesman.LOSE) : (gamesman.LOSE , min)    ,
        (gamesman.LOSE , gamesman.TIE)  : (gamesman.LOSE , first)  ,
        (gamesman.TIE  , gamesman.LOSE) : (gamesman.LOSE , second) ,
        (gamesman.LOSE , gamesman.WIN)  : (gamesman.LOSE , first)  ,
        (gamesman.WIN  , gamesman.LOSE) : (gamesman.LOSE , second) ,
        (gamesman.TIE  , gamesman.TIE)  : (gamesman.TIE  , min)    ,
        (gamesman.WIN  , gamesman.TIE)  : (gamesman.TIE  , second) ,
        (gamesman.TIE  , gamesman.WIN)  : (gamesman.TIE  , first)  ,
        (gamesman.WIN  , gamesman.WIN)  : (gamesman.WIN  , max)    ,
    }

    def __init__(self, count, seen=0, best=None):
        self.child_count = count
        self.children_observed = seen
        self.best_value = best or (gamesman.WIN, 0)

    def __repr__(self):
        return 'ChildValAccumulator(count={!r}, seen={!r}, best={!r})'.format(self.child_count, self.children_observed, self.best_value)

    def update(self, child_val):
        self.children_observed += 1
        v_now, r_now = self.best_value
        v_new, r_new = child_val
        v, f = ChildValAccumulator.REDUCTION_TABLE[v_now, v_new]
        r = f(r_now, r_new)
        self.best_value = (v, r)

    def done(self):
        return self.children_observed == self.child_count

    def finish(self):
        v, r = self.best_value
        if v == gamesman.LOSE:
            return (gamesman.WIN, r + 1)
        elif v == gamesman.WIN:
            return (gamesman.LOSE, r + 1)
        else:
            return (v, r + 1)

def main():
    name, game = gamesman.load_game_from_args('Solve games using MPI.')

    def maybePrimitive(pos):
        value = game.primitive(pos)
        if value is gamesman.UNDECIDED:
            recurseDown(pos, None)
        else:
            solved(pos, (value, 0))

    @keyReduce
    def recurseDown(pos, old, new):
        if old is None:
            # We have a new position. Expand one layer of the tree.
            moves = game.generateMoves(pos)
            children = set([game.doMove(pos, move) for move in moves])
            solving(pos, ChildValAccumulator(len(children)))
            for child in children:
                childToParents(child, ('parent', pos))
                maybePrimitive(child)
            # Record that we've already visited this subtree.
        return True

    @keyReduce
    def solved(pos, old, new):
        # We should only get here multiple times for primitives.
        assert old is None or old == new
        if old is None and new[0] != gamesman.DRAW:
            childToParents(pos, ('value', new))
        return new

    @keyReduce
    def childToParents(pos, old, new):
        if old is None:
            old = (None, [])
        old_val, old_parents = old
        kind, val = new
        if kind == 'value':
            assert old_val is None
            for parent in old_parents:
                # We already have some parents, so send them the new value.
                solving(parent, val)
            return (val, old_parents)
        else:
            assert kind == 'parent'
            parent = val
            # We have a new parent.
            old_parents.append(parent)
            if old_val is not None:
                # We already received the value, so send it to the new parent.
                solving(parent, old_val)
            return old

    @keyReduce
    def solving(pos, old, new):
        if old is None:
            return new
        else:
            old.update(new)
            if old.done():
                solved(pos, old.finish())
            return old

    if COMM_RANK == 0:
        # Only one process needs to inject the initialPosition.
        maybePrimitive(game.initialPosition)

    KeyReducer.router.run_until_done()
    #KeyReducer.router.print_all()

    @solving.foreach
    def markDraw(pos, acc):
        if not acc.done():
            if acc.children_observed != 0:
                print('pos', repr(pos), 'best', acc.best_value, acc.children_observed, '/', acc.child_count)
                print('pos', repr(pos), 'children', [game.doMove(pos, m) for m in game.generateMoves(pos)])
            solved(pos, (gamesman.DRAW, gamesman.DRAW_REMOTENESS))

    solved.save('results/{}/solved'.format(name))
    childToParents.save('results/{}/childToParents'.format(name))
    solving.save('results/{}/solving'.format(name))

if __name__ == '__main__':
    main()
