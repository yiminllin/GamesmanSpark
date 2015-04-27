README for TicTacToe

To run, just type
'make ttt' on the terminal.

Then, it will prompt you asking you would like to challenge a human or computer.

An example to place your piece into the middle would result in you typing '22'.
To make place your piece in the top middle you would type '12'.



README for SparkSolver

To test if you properly can run spark, enter
'python Make.py test' on the terminal.

If you see the output "RMSE: 15.7746" then you can run spark.

To run the solver for TicTacToe, just type
'python Make.py' on the terminal.

This will produce 3 .txt files named "TicTacToeFinalMappingOutput.txt" and "TicTacToePrimitives.txt":
    -"TicTacToeFinalMappingOutput.txt" contains all the positions with their respective Board Level, Game State, Remoteness, and Parent.
    (fastest number of moves to get to that position) and the position's parents.
    -"TicTacToePrimitives.txt" contains all the primitives for TicTacToe so we can verify the mapper works