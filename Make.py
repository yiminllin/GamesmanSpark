import sys
import subprocess
"""
Our sparksolve runs by putting spark-submit SparkSolver.py width height filename(without .py)
packagename local[#of computers you have]<--- one if running on local computer
"""
dictionary = {"ten": ["time", "spark-submit", "SparkSolver.py", "0", "10", ".ten",
			  "Ten", "local[1]"],
			  "ten2": ["time", "spark-submit", "SparkSolver.py", "0", "20", ".ten", "Ten",
			  "local[1]"],
			  "ttt": ["time", "spark-submit", "SparkSolver.py", "3", "3", ".ttt", "TicTacToe",
			  "local[1]"],
			  "test": ["time", "spark-submit", "SparkTest.py", "local[1]"]}

def main():
	#Default game is TicTacToe
	if (len(sys.argv) == 1):
		subprocess.call(dictionary["ttt"])
	else:
		nameOfGame = sys.argv[1]
		try: 
			commandForGame = dictionary[nameOfGame]
		except KeyError, e:
			print("Do not have a command for the game: " + nameOfGame)
			return

		subprocess.call(commandForGame)

if __name__ == "__main__":
	main()