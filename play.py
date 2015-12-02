import imp
import argparse
import os
import gamesman
import random

def main():
	parser = argparse.ArgumentParser(description='Play games')
	parser.add_argument('game', help='The path to the game script to run.')
	arg = parser.parse_args()
	name = os.path.split(os.path.splitext(arg.game)[0])[-1]
	game = imp.load_source(name, arg.game)
	print("\n     Welcome to GamesmanSpark " + name + "\n\n")
	while (True):
		play_against, ai_type, players_turn = game_setup()
		game_loop(game, play_against, ai_type, players_turn)
		if (end_game()):
			break;
			
def game_setup():
	play_against = -1
	ai_type = -1
	players_turn = -1
	while (play_against not in [1, 2]):
		play_against = int(raw_input("=== Play against computer or another player? ===\n" +
									"1) computer\n" +
									"2) player\n"))
	if (play_against == 1):
		while (ai_type not in [1, 2]):
			ai_type = int(raw_input("=== Please choose an AI type ===\n" +
									"1) Naive solver (Slow)\n" +
									"2) Random do moves \n"))
	print("============ Who move first? =============")
	print("1) Computer first.")
	print("2) I first.")
	print("3) Random.")
	players_turn = int(raw_input())
	if (players_turn == 3):
		players_turn = random.choice([1, 2])
	if (players_turn == 1):
		print("\n === Wait for computer's move === \n")
	return (play_against-1, ai_type-1, players_turn-1)
			
def game_loop(game, play_against, ai_type, players_turn):
	pos = game.initialPosition
	while (True):
		game.printBoard(pos)
		if game.primitive(pos) is not gamesman.UNDECIDED:
			if (players_turn):
				print("==== You lose. ====")
			else:
				print("==== You win. ====")
			break
		if (players_turn):
			player_move = (-1, -1)
			validMoves = game.generateMoves(pos)
			while (player_move not in validMoves):
				print("Possible valid moves:" + str([game.toIndex(l) for l in validMoves]))
				player_move = game.toLoc(int(raw_input("Please enter your move: ")))
			pos = game.doMove(pos, player_move)
			print("Your Move:" + str(player_move))
		else:
			print("\n === Wait for computer's move ===")
			computer_moves = game.generateMoves(pos)
			computer_move = -1
			if ai_type == 0:
				for m in computer_moves:
					s = solve(game, game.doMove(pos, m))
					if s is gamesman.LOSE:
						computer_move = m
						break
					elif s is gamesman.TIE:
						computer_move = m
				if computer_move == -1:
					computer_move = random.choice(computer_moves)
			elif ai_type == 1:
				computer_move = random.choice(computer_moves)
			pos = game.doMove(pos, computer_move)
			print("Computer's Move:" + str(computer_move) + "\n")
		if not play_against:
			players_turn = ~players_turn & 1
			
def end_game():
	print("Play again? y/n")
	user_input = raw_input()
	if (user_input == "n"):
		return 1
	return 0
			
def solve(game, pos):
	v = game.primitive(pos)
	if v is not gamesman.UNDECIDED:
		return v
	else:
		moves = game.generateMoves(pos)
		vals = [solve(game, game.doMove(pos, m)) for m in moves]
		if gamesman.LOSE in vals:
			return gamesman.WIN
		elif gamesman.TIE in vals:
			return gamesman.TIE
		else:
			return gamesman.LOSE

if __name__ == '__main__':
	main()