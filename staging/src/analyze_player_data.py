import pandas as pd
import numpy as np
import os
import sqlite3
import math
from constant_values import get_passing_stat_ids, get_qb_stat_ids_to_sql
from scipy import stats
from matplotlib import pyplot as plt
import csv


# ==== Output Files =====
CURRENT_FILE_DIR = 'os.path.dirname(os.path.realpath(__file__))'
DATA_DIR = '../../data'
FILENAME = 'qb_result_info.csv'
OUTPUT_FILE = os.path.join(DATA_DIR, FILENAME)
DATABASE_FILENAME = 'nfl_road_statistics.db'
DATABASE_PATH = os.path.join(DATA_DIR, DATABASE_FILENAME)
conn = sqlite3.connect(DATABASE_PATH)
print('OPENED CONNECTION')


# ==== Statistic Calculations =====
STAT_KEYS = ['home_mean', 'road_mean', 'home_std', 'road_std', 't_stat', 'p_val', 'difference']
def _get_means(value_distribution):
	return [np.mean(value_distribution['home_values']), np.mean(value_distribution['road_values'])]

def _get_stds(value_distribution):
	return [np.std(value_distribution['home_values']), np.std(value_distribution['road_values'])]

def _get_2_sample_t_results(value_distribution):
	return stats.ttest_ind(value_distribution['home_values'], value_distribution['road_values'])


# ==== Query QB Statistics =====
def get_sql_statement(key_value_dict):
	statement = '''SELECT * FROM qb_statistics'''
	if len(key_value_dict) > 0:
		statement += ' WHERE'
		for key in key_value_dict:
			if type(key_value_dict[key]['value']) == str:
				statement += ' %s %s %s AND' % (key, str(key_value_dict[key]['operation']), "'" + str(key_value_dict[key]['value']) + "'")
			else:
				statement += ' %s %s %s AND' % (key, str(key_value_dict[key]['operation']), str(key_value_dict[key]['value']))
		statement = statement[:-4]

	return statement + ';'


def get_road_qb_games(key_value_dict):
	key_value_dict['GAME_LOCATION'] = {
		'value': '@',
		'operation': '='
	}

	statement = get_sql_statement(key_value_dict)

	road_qb_games = pd.read_sql_query(statement, conn)
	print('Road QB shape: ' + str(road_qb_games.shape))
	return road_qb_games


def get_home_qb_games(key_value_dict):
	key_value_dict['GAME_LOCATION'] = {
		'value': '',
		'operation': '='
	}
	statement = get_sql_statement(key_value_dict)

	home_qb_games = pd.read_sql_query(statement, conn)
	print('Home QB shape: ' + str(home_qb_games.shape))
	return home_qb_games


def get_value_distributions(road_qb_games, home_qb_games, statistic):
	return {
	'home_values': np.array(home_qb_games[statistic]),
	'road_values': np.array(road_qb_games[statistic])
	}


# ==== Display Distribution Info =====
def get_value_distribution_stats(value_distribution):
	t_stat, pval = _get_2_sample_t_results(value_distribution)
	return {
	'home_mean': _get_means(value_distribution)[0],
	'road_mean': _get_means(value_distribution)[1],
	'home_std': _get_stds(value_distribution)[0],
	'road_std': _get_stds(value_distribution)[1],
	't_stat': t_stat,
	'p_val': pval,
	'difference': _get_means(value_distribution)[0] - _get_means(value_distribution)[1]
	}


def display_distributions(value_distribution, stat_id):
	home_values = value_distribution['home_values']
	road_values = value_distribution['road_values']
	plt.hist(home_values, alpha=0.5, label='Home')
	plt.hist(road_values, alpha=0.5, label='Road')
	plt.title(stat_id)
	plt.legend(loc='upper right')
	plt.show()


# ==== Organize Inputs =====
def get_key_value_dict():
	return {
	'PASS_ATTEMPTS': {
	'value': 10,
	'operation': '>='
	},
	'YEAR': {
	'value': 2018,
	'operation': '='
	}
	}

# ==== Write Results =====
def write_query_info_to_csv(csv_writer):
	print('Writing Query Info')
	key_value_dict = get_key_value_dict()
	for k in key_value_dict:
		csv_writer.writerow([k, key_value_dict[k]['operation'], key_value_dict[k]['value']])
	header_row = ['stat_id']
	header_row.extend(STAT_KEYS)
	csv_writer.writerow([])
	csv_writer.writerow(header_row)


def write_stat_to_csv(stat_vals, stat_id, csv_writer):
	print('Writing Stat info')
	stat_row = [stat_vals[key] for key in STAT_KEYS]
	stat_row.insert(0, stat_id)
	csv_writer.writerow(stat_row)


if __name__ == '__main__':
	road_qb_games = get_road_qb_games(get_key_value_dict())
	home_qb_games = get_home_qb_games(get_key_value_dict())
	qb_stat_to_sql = get_qb_stat_ids_to_sql()
	csv_file = open(OUTPUT_FILE, 'a')
	csv_writer = csv.writer(csv_file)
	write_query_info_to_csv(csv_writer)
	for stat_id in get_passing_stat_ids():
		value_distribution = get_value_distributions(road_qb_games, home_qb_games, qb_stat_to_sql[stat_id])
		stat_vals = get_value_distribution_stats(value_distribution)
		write_stat_to_csv(stat_vals, stat_id, csv_writer)
		# display_distributions(value_distribution, stat_id)

	csv_writer.writerow([])
	csv_writer.writerow([])
	csv_file.close()

