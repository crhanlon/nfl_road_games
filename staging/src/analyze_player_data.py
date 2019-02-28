import pandas as pd
import numpy as np
import os
import sqlite3
import math


# ==== Output Files =====
CURRENT_FILE_DIR = 'os.path.dirname(os.path.realpath(__file__))'
DATA_DIR = '../../data'
FILENAME = 'qb_statistics.csv'
OUTPUT_FILE = os.path.join(DATA_DIR, FILENAME)
DATABASE_FILENAME = 'nfl_road_statistics.db'
DATABASE_PATH = os.path.join(DATA_DIR, DATABASE_FILENAME)
conn = sqlite3.connect(DATABASE_PATH)
print('OPENED CONNECTION')


# ==== Statistic Calculations =====
def _get_means(value_distribution):
	return [np.mean(value_distribution['home_values']), np.mean(value_distribution['road_values'])]

def _get_stds(value_distribution):
	return [np.std(value_distribution['home_values']), np.std(value_distribution['road_values'])]

def _get_cohen_d(value_distribution):
	pooled_standard_dev = np.sqrt((np.std(value_distribution['home_values'])**2 + np.std(value_distribution['road_values'])**2)/2)
	return (np.mean(value_distribution['home_values']) - np.mean(value_distribution['road_values']))/pooled_standard_dev


# ==== Query QB Statistics =====
def get_road_qb_games():
	statement = '''SELECT * FROM qb_statistics WHERE GAME_LOCATION = '@';'''
	road_qb_games = pd.read_sql_query(statement, conn)
	print('Road QB shape: ' + str(road_qb_games.shape))
	return road_qb_games


def get_home_qb_games():
	statement = '''SELECT * FROM qb_statistics WHERE GAME_LOCATION = '';'''
	home_qb_games = pd.read_sql_query(statement, conn)
	print('Home QB shape: ' + str(home_qb_games.shape))
	return home_qb_games


def get_value_distributions(road_qb_games, home_qb_games, statistic):
	return {
	'home_values': np.array(home_qb_games[statistic]),
	'road_values': np.array(road_qb_games[statistic])
	}


# ==== Display Distribution Info =====
def print_value_distribution_stats(value_distribution):
	print('Home Stats')
	print('Mean: ' + str(_get_means(value_distribution)[0]))
	print('Standard Dev: ' + str(_get_stds(value_distribution)[0]))
	print('---------------------')
	print('Road Stats')
	print('Mean: ' + str(_get_means(value_distribution)[1]))
	print('Standard Dev: ' + str(_get_stds(value_distribution)[1]))
	print('---------------------')
	print("Cohen's d")
	print(str(_get_cohen_d(value_distribution)))


if __name__ == '__main__':
	road_qb_games = get_road_qb_games()
	home_qb_games = get_home_qb_games()
	value_distribution = get_value_distributions(road_qb_games, home_qb_games, 'PASS_RATING')
	print_value_distribution_stats(value_distribution)

