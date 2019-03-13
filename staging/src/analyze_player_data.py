import pandas as pd
import numpy as np
import os
import sqlite3
import math
from constant_values import get_passing_stat_ids, get_rushing_stat_ids, get_receiving_stat_ids, get_general_stat_ids, id_2_col, operation_2_str
from scipy import stats
from matplotlib import pyplot as plt
import csv
import time


# ==== Output Files =====
CURRENT_FILE_DIR = 'os.path.dirname(os.path.realpath(__file__))'
DATA_DIR = '../../data'
DATABASE_FILENAME = 'nfl_road_statistics.db'
DATABASE_PATH = os.path.join(DATA_DIR, 'raw', DATABASE_FILENAME)
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
def get_sql_statement(key_value_dict, data_type):
	table = 'passing_statistics'
	if data_type == 'rushing':
		table = 'rushing_statistics'
	elif data_type == 'receiving':
		table = 'receiving_statistics'

	statement = '''SELECT * FROM %s''' % table
	if len(key_value_dict) > 0:
		statement += ' WHERE'
		for key in key_value_dict:
			if type(key_value_dict[key]['value']) == str:
				statement += ' %s %s %s AND' % (key, str(key_value_dict[key]['operation']), "'" + str(key_value_dict[key]['value']) + "'")
			else:
				statement += ' %s %s %s AND' % (key, str(key_value_dict[key]['operation']), str(key_value_dict[key]['value']))
		statement = statement[:-4]

	return statement + ';'


def get_road_games(key_value_dict, data_type):
	key_value_dict['GAME_LOCATION'] = {
		'value': '@',
		'operation': '='
	}

	statement = get_sql_statement(key_value_dict, data_type)

	road_games = pd.read_sql_query(statement, conn)
	print('Road games shape: ' + str(road_games.shape))
	return road_games


def get_home_games(key_value_dict, data_type):
	key_value_dict['GAME_LOCATION'] = {
		'value': '',
		'operation': '='
	}
	statement = get_sql_statement(key_value_dict, data_type)

	home_games = pd.read_sql_query(statement, conn)
	print('Home games shape: ' + str(home_games.shape))
	return home_games


def get_value_distributions(road_qb_games, home_qb_games, statistic):
	return {
	'home_values': np.array(home_qb_games[statistic]),
	'road_values': np.array(road_qb_games[statistic])
	}


# ==== Display Distribution Info =====
def map_nan_to_val(new_val):
	home_values = value_distribution['home_values']
	road_values = value_distribution['road_values']
	for i in range(len(home_values)):
		if str(home_values[i]) == 'nan':
			home_values[i] = new_val
	for i in range(len(road_values)):
		if str(road_values[i]) == 'nan':
			road_values[i] = new_val


def get_value_distribution_stats(value_distribution):
	value_dist_copy = value_distribution.copy()
	map_nan_to_val(0)

	t_stat, pval = _get_2_sample_t_results(value_dist_copy)

	return {
	'home_mean': _get_means(value_dist_copy)[0],
	'road_mean': _get_means(value_dist_copy)[1],
	'home_std': _get_stds(value_dist_copy)[0],
	'road_std': _get_stds(value_dist_copy)[1],
	't_stat': t_stat,
	'p_val': pval,
	'difference': _get_means(value_dist_copy)[0] - _get_means(value_dist_copy)[1]
	}


def display_distributions(value_distribution, stat_id):
	home_values = value_distribution['home_values']
	road_values = value_distribution['road_values']
	plt.hist(home_values, alpha=0.5, label='Home')
	plt.hist(road_values, alpha=0.5, label='Road')
	plt.title(stat_id)
	plt.legend(loc='upper right')
	plt.show()

# ==== Write Results =====
def write_query_info_to_csv(csv_writer):
	key_value_dict = get_key_value_dict()
	for k in key_value_dict:
		csv_writer.writerow([k, key_value_dict[k]['operation'], key_value_dict[k]['value']])
	header_row = ['stat_id']
	header_row.extend(STAT_KEYS)
	csv_writer.writerow([])
	csv_writer.writerow(header_row)


def write_stat_to_csv(stat_vals, stat_id, csv_writer):
	stat_row = [stat_vals[key] for key in STAT_KEYS]
	stat_row.insert(0, stat_id)
	csv_writer.writerow(stat_row)


# ==== Organize Inputs =====
# CHANGE HERE
AUTOMATICALLY_OVERWRITE = True
DATA_TYPE = 'passing'

def get_key_value_dict_list():
	l = [
			{
				'PASS_ATTEMPTS': {
					'value': 10,
					'operation': '>='
				},
				'YEAR': {
					'value': 2008,
					'operation': '>='
				}
			}
		]
	for i in range(2008, 2019):
		l.append({
					'PASS_ATTEMPTS': {
					'value': 10,
					'operation': '>='
					},
					'YEAR': {
						'value': i,
						'operation': '='
					}
				})

	return l


def get_filename(key_value_dict):
	type_file_dir = os.path.join(DATA_DIR, 'interim', DATA_TYPE)
	filename = DATA_TYPE + '_'
	for key in key_value_dict:
		filename += key + '_'
		filename += operation_2_str(key_value_dict[key]['operation']) + '_' + str(key_value_dict[key]['value']) + '_'
	return os.path.join(type_file_dir, filename[:-1] + '.csv')


def get_stat_ids():
	if DATA_TYPE == 'passing':
		print('returning passing')
		return get_passing_stat_ids()
	elif DATA_TYPE == 'rushing':
		print('returning rushing')
		return get_rushing_stat_ids()
	elif DATA_TYPE == 'receiving':
		print('returning receiving')
		return get_receiving_stat_ids()
	raise Exception('Data Type Incorrect')
	return None


if __name__ == '__main__':

	for kvd in get_key_value_dict_list():
		output_file = get_filename(kvd)
		if os.path.isfile(output_file) and not AUTOMATICALLY_OVERWRITE:
			raise Exception('File already exists')
		road_qb_games = get_road_games(kvd.copy(), DATA_TYPE)
		home_qb_games = get_home_games(kvd.copy(), DATA_TYPE)
		stat_id_2_sql = id_2_col()
		csv_file = open(output_file, 'w')
		csv_writer = csv.writer(csv_file)
		write_query_info_to_csv(csv_writer)
		stat_id_list = get_stat_ids()
		for stat_id in stat_id_list:
			value_distribution = get_value_distributions(road_qb_games, home_qb_games, stat_id_2_sql[stat_id])
			if type(value_distribution['home_values'][0]) == str:
				continue
			stat_vals = get_value_distribution_stats(value_distribution)
			write_stat_to_csv(stat_vals, stat_id, csv_writer)

		csv_writer.writerow([])
		csv_writer.writerow([])
		csv_file.close()

