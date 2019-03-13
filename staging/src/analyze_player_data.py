import pandas as pd
import numpy as np
import os
import sqlite3
import math
from constant_values import get_passing_stat_ids, get_rushing_stat_ids, get_receiving_stat_ids, get_general_stat_ids, id_2_col, operation_2_str, year_2_idx
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
STAT_KEYS = ['home_mean', 'road_mean', 'home_std', 'road_std', 'home_count', 'road_count', 't_stat', 'p_val', 'difference', 'stat_type']
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
	return road_games


def get_home_games(key_value_dict, data_type):
	key_value_dict['GAME_LOCATION'] = {
		'value': '',
		'operation': '='
	}
	statement = get_sql_statement(key_value_dict, data_type)

	home_games = pd.read_sql_query(statement, conn)
	return home_games


def get_value_distributions(road_qb_games, home_qb_games, statistic):
	return {
	'home_values': np.array(home_qb_games[statistic]),
	'road_values': np.array(road_qb_games[statistic])
	}


# ==== Display Distribution Info =====
def map_nan_to_val(new_val, value_distribution):
	home_values = value_distribution['home_values']
	road_values = value_distribution['road_values']
	for i in range(len(home_values)):
		if str(home_values[i]) == 'nan':
			home_values[i] = new_val
	for i in range(len(road_values)):
		if str(road_values[i]) == 'nan':
			road_values[i] = new_val


def get_value_distribution_stats(value_distribution, stat_id):
	value_dist_copy = value_distribution.copy()
	map_nan_to_val(0, value_dist_copy)

	t_stat, pval = _get_2_sample_t_results(value_dist_copy)

	if stat_id in get_passing_stat_ids():
		stat_type = 'passing'
	elif stat_id in get_rushing_stat_ids():
		stat_type = 'rushing'
	elif stat_id in get_receiving_stat_ids():
		stat_type = 'receiving'
	else:
		raise Exception('Invalid Stat: ' + stat_id)

	return {
	'home_mean': _get_means(value_dist_copy)[0],
	'road_mean': _get_means(value_dist_copy)[1],
	'home_std': _get_stds(value_dist_copy)[0],
	'road_std': _get_stds(value_dist_copy)[1],
	'home_count': len(value_dist_copy['home_values']),
	'road_count': len(value_dist_copy['road_values']),
	't_stat': t_stat,
	'p_val': pval,
	'difference': _get_means(value_dist_copy)[0] - _get_means(value_dist_copy)[1],
	'stat_type': stat_type
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
def write_query_info_to_csv(csv_writer, key_value_dict):
	for k in key_value_dict:
		csv_writer.writerow([k, key_value_dict[k]['operation'], key_value_dict[k]['value']])
	header_row = ['stat_id']
	header_row.extend(STAT_KEYS)
	csv_writer.writerow([])
	csv_writer.writerow(header_row)


def write_stat_to_csv(stat_vals, stat_id, csv_writer):
	print(stat_id)
	stat_row = [stat_vals[key] for key in STAT_KEYS]
	stat_row.insert(0, stat_id)
	csv_writer.writerow(stat_row)


# ==== Organize Inputs =====
# CHANGE HERE
AUTOMATICALLY_OVERWRITE = True
DATA_TYPE = 'receiving'

def get_key_value_dict_list():
	if DATA_TYPE == 'receiving':
		l = [
				{
					'YEAR': {
						'value': 2008,
						'operation': '>='
					}
				}
			]
		for i in range(2008, 2019):
			l.append({
						'YEAR': {
							'value': i,
							'operation': '='
						}
					})
	elif DATA_TYPE == 'passing':
		l = [
				{
					'YEAR': {
						'value': 2008,
						'operation': '>='
					},
					'PASS_ATTEMPTS': {
						'value': 10,
						'operation': '>='
					}
				}
			]
		for i in range(2008, 2019):
			l.append({
						'YEAR': {
							'value': i,
							'operation': '='
						},
						'PASS_ATTEMPTS': {
							'value': 10,
							'operation': '>='
						}
					})
	elif DATA_TYPE == 'rushing':
		l = [
				{
					'YEAR': {
						'value': 2008,
						'operation': '>='
					},
					'RUSH_ATTEMPTS': {
						'value': 5,
						'operation': '>='
					}
				}
			]
		for i in range(2008, 2019):
			l.append({
						'YEAR': {
							'value': i,
							'operation': '='
						},
						'RUSH_ATTEMPTS': {
							'value': 5,
							'operation': '>='
						}
					})
	else:
		raise Excpetion('Bad DATA_TYPE')
	return l


def get_filename(key_value_dict):
	type_file_dir = os.path.join(DATA_DIR, 'interim', DATA_TYPE)
	filename = DATA_TYPE + '_'
	for key in key_value_dict:
		filename += key + '_'
		filename += operation_2_str()[key_value_dict[key]['operation']] + '_' + str(key_value_dict[key]['value']) + '_'
	return os.path.join(type_file_dir, filename[:-1] + '.csv')


def get_stat_ids():
	if DATA_TYPE == 'passing':
		return get_passing_stat_ids()
	elif DATA_TYPE == 'rushing':
		return get_rushing_stat_ids()
	elif DATA_TYPE == 'receiving':
		return get_receiving_stat_ids()
	raise Exception('Data Type Incorrect')
	return None


def write_all_aggregated_stats():
	global DATA_TYPE
	for dt in ['passing', 'rushing', 'receiving']:
		DATA_TYPE = dt
		for kvd in get_key_value_dict_list():
			output_file = get_filename(kvd)
			if os.path.isfile(output_file) and not AUTOMATICALLY_OVERWRITE:
				raise Exception('File already exists')
			road_qb_games = get_road_games(kvd.copy(), DATA_TYPE)
			home_qb_games = get_home_games(kvd.copy(), DATA_TYPE)
			stat_id_2_sql = id_2_col()
			csv_file = open(output_file, 'w')
			csv_writer = csv.writer(csv_file)
			write_query_info_to_csv(csv_writer, kvd.copy())
			stat_id_list = get_stat_ids()
			for stat_id in stat_id_list:
				value_distribution = get_value_distributions(road_qb_games, home_qb_games, stat_id_2_sql[stat_id])
				if type(value_distribution['home_values'][0]) == str:
					continue
				stat_vals = get_value_distribution_stats(value_distribution, stat_id)
				write_stat_to_csv(stat_vals, stat_id, csv_writer)

			csv_writer.writerow([])
			csv_writer.writerow([])
			csv_file.close()


# ==== Temporal Functions =====

def get_all_temporal_stats():
	temporal_dict = {}
	global DATA_TYPE
	for dt in ['passing', 'rushing', 'receiving']:
		DATA_TYPE = dt
		for kvd in get_key_value_dict_list():
			year_val = kvd['YEAR']['value']
			if kvd['YEAR']['operation'] != '=':
				year_val = 'all'
			road_qb_games = get_road_games(kvd.copy(), DATA_TYPE)
			home_qb_games = get_home_games(kvd.copy(), DATA_TYPE)
			stat_id_2_sql = id_2_col()
			stat_id_list = get_stat_ids()
			for stat_id in stat_id_list:
				value_distribution = get_value_distributions(road_qb_games, home_qb_games, stat_id_2_sql[stat_id])
				if type(value_distribution['home_values'][0]) == str:
					continue
				stat_vals = get_value_distribution_stats(value_distribution, stat_id)
				if stat_id not in temporal_dict:
					temporal_dict[stat_id] = []
				temporal_dict[stat_id].append({
						'year': year_val,
						'home_mean': stat_vals['home_mean'],
						'road_mean': stat_vals['road_mean'],
						'home_std': stat_vals['home_std'],
						'road_std': stat_vals['road_std'],
						'home_count': stat_vals['home_count'],
						'road_count': stat_vals['road_count'],
						't_stat': stat_vals['t_stat'],
						'p_val': stat_vals['p_val'],
						'difference': stat_vals['difference'],
						'data_type': dt
				})
	return temporal_dict


def get_temporal_filename(data_type):
	return os.path.join(DATA_DIR, 'interim', data_type, 'temporal_data.csv')


def write_temporal_stat_row(writer, ts, stat_id):
	y2i = year_2_idx()
	val_dict = {}

	for k in ts[0].keys():
		if k != 'year' and k != 'data_type':
			val_dict[k] = [0]*14
			val_dict[k][0] = stat_id
			val_dict[k][1] = k

	for y in ts:
		year = str(y['year'])
		for val in y.keys():
			if val != 'year' and val != 'data_type':
				val_dict[val][y2i[year]] = y[val]

	for v in val_dict:
		writer.writerow(val_dict[v])


def write_all_temporal_stats():
	temporal_stats = get_all_temporal_stats()
	with open(get_temporal_filename('passing'), 'w') as f_pass:
		with open(get_temporal_filename('rushing'), 'w') as f_rush:
			with open(get_temporal_filename('receiving'), 'w') as f_rec:
				pass_writer = csv.writer(f_pass)
				rush_writer = csv.writer(f_rush)
				rec_writer = csv.writer(f_rec)
				header_row = ['stat_id', 'value', 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 'all']
				pass_writer.writerow(header_row)
				rush_writer.writerow(header_row)
				rec_writer.writerow(header_row)
				print(temporal_stats.keys())
				for stat_id in temporal_stats:
					if temporal_stats[stat_id][0]['data_type'] == 'passing':
						write_temporal_stat_row(pass_writer, temporal_stats[stat_id], stat_id)
					elif temporal_stats[stat_id][0]['data_type'] == 'rushing':
						write_temporal_stat_row(rush_writer, temporal_stats[stat_id], stat_id)
					elif temporal_stats[stat_id][0]['data_type'] == 'receiving':
						write_temporal_stat_row(rec_writer, temporal_stats[stat_id], stat_id)
					else:
						raise Exception('Invalid Data Type')



if __name__ == '__main__':
	# write_all_aggregated_stats()
	# get_all_temporal_stats()
	write_all_temporal_stats()

