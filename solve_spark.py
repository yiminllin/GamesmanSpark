from __future__ import print_function
import imp
import argparse
import os
import gamesman

'''
Algorithm works as follows:
Two passes:
 Down pass
 Up pass
In the down pass, we're constructing at least two RDDs (and usually three).

child_to_parent :: [(C, P)], C is not unique.
Can be skipped if the game implements an UndoMoves.

unsolved :: [(P, ((best_value, best_remoteness), number_seen_children,
                  number_children))], P is unique.

primitives :: [(P, (final_value, final_remoteness))]

In the upwards pass, we use the above RDDs and another two more, frontier and
solved.

Both are of the form:

frontier :: [(P, (final_value, final_remoteness))]

Initially:

frontier = primitives
solved = []

Ultimately:

    frontier is empty
    everything still in unsolved is moved to solved and becomes draws 
'''

class DownPass(object):

    def __init__(self, sc, game):
        self.sc = sc
        self.game = game
        self.down_frontier = sc.parallelize([game.initialPosition])
        self.child_to_parent = sc.parallelize([])
        self.unsolved = sc.parallelize([])
        self.solved = sc.parallelize([])

    def finish(self):
        #print('stepping to finish')
        while not self.down_frontier.isEmpty():
            self.step()

    def step(self):
        game = self.game
        def gen_children(t):
            p, (v, r) = t
            print('generateMoves({})'.format(p))
            return (p, [game.doMove(p, m) for m in game.generateMoves(p)])
        def make_up(t):
            #print('make_up', t)
            p, cs = t
            for c in cs:
                yield (c, p)
        def make_unsolved(t):
            #print('make unsolved', t)
            p, cs = t
            return (p, ((gamesman.WIN, 0), 0, len(cs)))
        def is_solved(t):
            #print('is solved', t)
            p, (v, r) = t
            if v == gamesman.UNDECIDED:
                return False
            else:
                return True
        def gen_primitive(p):
            print('primitive({})'.format(p))
            return (p, (game.primitive(p), 0))


        # Invariant:
        # Each P is in at most one of solved, unsolved, and down_frontier
        # primitive_vals :: [(P, (V, 0))]
        primitive_vals = self.down_frontier.map(gen_primitive)

        # Invariant protected, each P in at most one of
        # frontier_with_children and new_primitives.
        new_primitives = primitive_vals.filter(is_solved)
        frontier_with_children = primitive_vals.filter(lambda t: not is_solved(t))

        # parent_to_children :: [(P, [C])]
        parent_to_children = frontier_with_children.map(gen_children)
        parent_to_children.cache()

        # new_child_links :: [(C, P)]
        new_child_links = parent_to_children.flatMap(make_up)
        # new_unsolved :: [(P, ((WIN, 0), 0, int)]
        new_unsolved = parent_to_children.map(make_unsolved)
        # new_down_frontier :: [C]

        self.child_to_parent = self.child_to_parent.union(new_child_links)
        self.unsolved = self.unsolved.union(new_unsolved)
        self.solved = self.solved.union(new_primitives)
        self.child_to_parent.cache()
        self.unsolved.cache()
        self.solved.cache()

        # Protect invariant
        new_down_frontier = parent_to_children.flatMap(lambda t: zip(t[1], t[1])).distinct()
        new_down_frontier = new_down_frontier.subtractByKey(self.solved)
        new_down_frontier = new_down_frontier.subtractByKey(self.unsolved)
        self.down_frontier = new_down_frontier.keys()
        self.down_frontier.cache()

        print('one step down')
        print('child_to_parent', self.child_to_parent.collect())
        print('unsolved', self.unsolved.collect())
        print('solved', self.solved.collect())
        print('down_frontier', self.down_frontier.collect())

class UpPass(object):
    def __init__(self, sc, game, down_pass):
        def rotate_parent_to_key(t):
            c, ((v, r), p) = t
            return (p, ((v, r), 1, None))
        def non_none(a, b):
            if a == None:
                return b
            else:
                return a
        first = lambda a, b: a
        second = lambda a, b: b
        vs = {
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
        def reduce_child_vals(t1, t2):
            print('reduce', t1, t2)
            ((v1, r1), n1, N1) = t1
            ((v2, r2), n2, N2) = t2

            N = non_none(N1, N2)
            n = n1 + n2
            v, f = vs[(v1, v2)]
            r = f(r1, r2)
            return ((v, r), n, N)
        def done(t):
            (p, ((v, r), n, N)) = t
            return n == N
        def finish(t):
            print('finish', t)
            ((v, rc), n, N) = t
            r = rc + 1
            if v == gamesman.LOSE:
                return (gamesman.WIN, r)
            elif v == gamesman.WIN:
                return (gamesman.LOSE, r)
            else:
                return (v, r)
        def unsolved_to_draw(t):
            return (gamesman.DRAW, None)
        frontier = down_pass.solved
        solved = down_pass.solved
        unsolved = down_pass.unsolved
        child_to_parent = down_pass.child_to_parent
        while not frontier.isEmpty():
            parents_with_child_vals = frontier.join(child_to_parent)
            parents_to_child_vals = parents_with_child_vals.map(rotate_parent_to_key)
            new_unsolved = unsolved.union(parents_to_child_vals).reduceByKey(reduce_child_vals)
            new_unsolved.cache()
            unsolved_done = new_unsolved.filter(done)
            new_unsolved_not_done = new_unsolved.filter(lambda t: not done(t))
            
            frontier = unsolved_done.mapValues(finish)
            frontier.cache()

            solved = solved.union(frontier)
            unsolved = new_unsolved_not_done

            solved.cache()
            unsolved.cache()
            print('one step up')
            print('frontier', frontier.collect())
            print('solved', solved.collect())
            print('unsolved', unsolved.collect())

        draws = unsolved.mapValues(unsolved_to_draw)
        solved = solved.union(draws)
        self.solved = solved

def main():
    parser = argparse.ArgumentParser(description='Naively solves games')
    parser.add_argument('game', help='The path to the game script to run.')
    arg = parser.parse_args()
    name = os.path.split(os.path.splitext(arg.game)[0])[-1]
    game = imp.load_source(name, arg.game)

    import pyspark
    sc = pyspark.SparkContext("local", "GamesmanSpark")
    first = DownPass(sc, game)
    first.finish()
    first.solved.saveAsTextFile('results/{}/first/solved'.format(name))
    first.unsolved.saveAsTextFile('results/{}/first/unsolved'.format(name))
    first.child_to_parent.saveAsTextFile('results/{}/first/child_to_parent'.format(name))
    second = UpPass(sc, game, first)
    second.solved.saveAsTextFile('results/{}/second/solved'.format(name))

if __name__ == '__main__':
    main()
