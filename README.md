README for TicTacToe

To run, just type
'make ttt' on the terminal.

Then, it will prompt you asking you would like to challenge a human or computer.

An example to place your piece into the middle would result in you typing '22'.
To make place your piece in the top middle you would type '12'.



README for SparkSolver

To test if you properly can run spark, enter
'make test' on the terminal.

If you see the output "RMSE: 15.7746" then you can run spark.

To run the solver for TicTacToe, just type
'make' on the terminal.

This will produce 3 .txt files named "output", "primitives", and "testing":
    -"output" contains all the positions with their respective Board Level 
    (fastest number of moves to get to that position) and the position's parents.
    -"primitives" contains all the primitives for TicTacToe so we can verify the mapper works
    -"testing" is a file created to check to see if we can verify the board's winner