MASTER="local[8]"
SOLVER=SparkSolver.py

.PHONY: clean

default: beta

beta:
	PYTHONWARNINGS="ignore" time spark-submit $(SOLVER) 3 3 .ttt TicTacToe

test:
	PYTHONWARNINGS="ignore" time spark-submit SparkTest.py

ttt:
	PYTHONWARNINGS="ignore" python TicTacToe/ttt.py

connect4:
	PYTHONWARNINGS="ignore" time spark-submit $(SOLVER) 7 6 .connect4 Connect4

clean:
	rm -rf *.pyc
