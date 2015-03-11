from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):
    """
    Takes in a key, value pair of (board state, level), creates
    all of the children of that board state if is on the same level as the
    global level, and returns them in a list.
    """
    """ YOUR CODE HERE """
    child_list = []
    #Check if we are at the right level, so then we can make children of only those boards
    if value[1] == level:
        temp = Sliding.hash_to_board(WIDTH, HEIGHT, value[0])
        iter_list = Sliding.children(WIDTH, HEIGHT, temp)
        for child in iter_list:
            child_list += [(Sliding.board_to_hash(WIDTH,HEIGHT, child), level + 1)]
    #Spark map only lets us return a list if we want multiple things.
    #Unlike Hadoop I believe which allows us to emit
    return child_list

def bfs_reduce(value1, value2):
    """
    Takes in two levels from the same board state and returns the
    smaller of the two.
    """
    """ YOUR CODE HERE """
    return min(value1, value2)

def filter_func(x):
    """
    Helper function used by the filter transformation.
    Takes in a key, value pair of (board state, level) and returns
    True if the level of the board is equal to the global level.
    This helps to find if we have any new children in our RDD.
    """
    return x[1] == level

def solve_puzzle(master, output, height, width, slaves):
    global HEIGHT, WIDTH, level
    HEIGHT=height
    WIDTH=width
    level = 0

    sc = SparkContext(master, "python")

    sol = Sliding.solution(WIDTH, HEIGHT)

    """ YOUR CODE HERE """
    sol = Sliding.board_to_hash(WIDTH, HEIGHT, sol)
    new_visited = [(sol, level)]
    
    new_visited = sc.parallelize(new_visited)
    num = 1

    #while there are still (k, v) pairs at the current level
    while num:
        #use += as we do not retain board sets not at the global level
        #in our mapping function
        new_visited += new_visited.flatMap(bfs_map)
        if level % 4 == 3: # only reduce and filter every other iteration for performance reasons
            new_visited = new_visited.reduceByKey(bfs_reduce)
            new_visited = new_visited.partitionBy(PARTITION_COUNT) #figure out how to use hash
            num = new_visited.filter(filter_func).count() # count the number of elements in the RDD at the current level
        level += 1
        # Debuggin purposes print("\n\n\nLevel " + str(level) + '\n\n\n')

    """ YOUR OUTPUT CODE HERE """
    new_visited.coalesce(slaves).saveAsTextFile(output)

    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    parser.add_argument("-S", "--slaves", type=int, default=6,
            help="number of slaves executing the job")
    args = parser.parse_args()

    global PARTITION_COUNT
    PARTITION_COUNT = args.slaves * 16

    # call the puzzle solver
    solve_puzzle(args.master, args.output, args.height, args.width, args.slaves)

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
