from pyspark import SparkContext
from ttt import initiateBoard, generateMove

n = 3

def bfs_map(value):
    children = generateMove(value[0], n)
    return children

def bfs_reduce(value1, value2):
    return ' '


def printFunction(rdd, fName):
    output_file = open(fName, "w")
    writer = lambda line: output_file.write(str(line) + "\n")
    compile_together = rdd.collect()
    #Running through the list and formatting it as level and then board set
    for elem in compile_together:
        writer(str(elem[1]) + " " + str(elem[0]))

def main():
    print("Hey")
    sc = SparkContext("local[1]", "python")

    blankBoard = initiateBoard(n)

    rdd = [[blankBoard, None]]
    rdd = sc.parallelize(rdd)

    # finishedLevel = rdd.flatMap(bfs_map)
    # rdd += finishedLevel

    printFunction(rdd, "test.txt")


if __name__ == "__main__":
    main()
