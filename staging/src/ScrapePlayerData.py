import requests
import lxml.html as lh
import pandas as pd
import numpy as np
import os
import sqlite3
from constant_values import get_general_stat_ids, get_type_dict, id_2_col, get_qb_sql_cols, get_passing_stat_ids, create_passing_table, get_rushing_stat_ids, get_rushing_sql_cols, create_rushing_table, get_receiving_stat_ids, get_receiving_sql_cols, create_receiving_table


# ==== Output Files =====
CURRENT_FILE_DIR = 'os.path.dirname(os.path.realpath(__file__))'
DATA_DIR = '../../data'
DATABASE_FILENAME = 'nfl_road_statistics.db'
DATABASE_PATH = os.path.join(DATA_DIR, DATABASE_FILENAME)
conn = sqlite3.connect(DATABASE_PATH)
print('OPENED CONNECTION')


# ==== Initialize Table Setup =====
def create_player_table():
	pc_cursor = conn.cursor()
	pc_cursor.execute('''CREATE TABLE IF NOT EXISTS player_info
		(PLAYER_ID INT PRIMARY_KEY,
		PLAYER_NAME TEXT,
		POSITION TEXT,
		PLAYER_URL TEXT);''')
	conn.commit()
	print('Created Player Table')


def create_tables():
	create_passing_table(conn)
	create_rushing_table(conn)
	create_receiving_table(conn)
	create_player_table()


def drop_all_tables():
	drop_cursor = conn.cursor()
	drop_cursor.execute('''DROP TABLE IF EXISTS qb_statistics''')
	drop_cursor.execute('''DROP TABLE IF EXISTS passing_statistics''')
	drop_cursor.execute('''DROP TABLE IF EXISTS rushing_statistics''')
	drop_cursor.execute('''DROP TABLE IF EXISTS receiving_statistics''')
	drop_cursor.execute('''DROP TABLE IF EXISTS player_info''')


# ==== Constants =====
GENERAL_STAT_IDS = get_general_stat_ids()
PASSING_STAT_IDS = get_passing_stat_ids()
RUSHING_STAT_IDS = get_rushing_stat_ids()
RECEIVING_STAT_IDS = get_receiving_stat_ids()

ID2COL = id_2_col()
TYPE_DICT = get_type_dict()

GAME_ID_NUM = -1
PLAYER_ID_NUM = -1


def update_ids():
	global GAME_ID_NUM, PLAYER_ID_NUM
	id_cursor = conn.cursor()
	pass_game_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(GAME_ID) FROM passing_statistics''')][0]
	rush_game_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(GAME_ID) FROM rushing_statistics''')][0]
	receive_game_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(GAME_ID) FROM receiving_statistics''')][0]
	if pass_game_id != None:
		GAME_ID_NUM = pass_game_id
	if rush_game_id != None and rush_game_id > GAME_ID_NUM:
		GAME_ID_NUM = rush_game_id
	if receive_game_id != None and receive_game_id > GAME_ID_NUM:
		GAME_ID_NUM = receive_game_id

	pass_player_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(PLAYER_ID) FROM passing_statistics''')][0]
	rush_player_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(PLAYER_ID) FROM rushing_statistics''')][0]
	receive_player_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(PLAYER_ID) FROM receiving_statistics''')][0]
	if pass_player_id != None:
		PLAYER_ID_NUM = pass_player_id
	if rush_player_id != None and rush_player_id > PLAYER_ID_NUM:
		PLAYER_ID_NUM = rush_player_id
	if receive_player_id != None and receive_player_id > PLAYER_ID_NUM:
		PLAYER_ID_NUM = receive_player_id


def initialize_values(drop_tables=False):
	if drop_tables:
		drop_all_tables()
	create_tables()
	update_ids()


# ==== Player Scraping Functions =====
def isStatRow(row_items):
	isValidRow = False
	for elem in row_items:
		if elem[0] == 'id' and 'stats' in elem[1]:
			isValidRow = True
	return isValidRow


def isPlayoffRow(row_items):
	isPRow = False
	for elem in row_items:
		if elem[0] == 'id' and 'playoffs' in elem[1]:
			isPRow = True
	return int(isPRow)


def is_passing_stat(item):
	return item in PASSING_STAT_IDS or item in GENERAL_STAT_IDS

def is_rushing_stat(item):
	return item in RUSHING_STAT_IDS or item in GENERAL_STAT_IDS

def is_receiving_stat(item):
	return item in RECEIVING_STAT_IDS or item in GENERAL_STAT_IDS


def addStatToDict(col_items, text, game_dict, data_type):
	global TYPE_DICT
	columns = GENERAL_STAT_IDS[:]
	if data_type == 'passing':
		columns.extend(PASSING_STAT_IDS)
	elif data_type == 'rushing':
		columns.extend(RUSHING_STAT_IDS)
	elif data_type == 'receiving':
		columns.extend(RECEIVING_STAT_IDS)

	for item in col_items:
		if item[0] == 'data-stat' and item[1] in columns:
			type_val = TYPE_DICT[item[1]]
			if type_val == str:
				game_dict[item[1]] = text
			elif type_val == int or type_val == float:
				if text == '':
					game_dict[item[1]] = None
				else:
					game_dict[item[1]] = type_val(text)


def get_player_name(doc):
	h1_elems = doc.xpath('//h1')
	for i in range(len(h1_elems)):
		h1_elem = h1_elems[i]
		for item in h1_elem.items():
			if item[1] == 'name':
				return h1_elems[i].text_content()
	return None

def get_player_position(doc):
	p_elems = doc.xpath('//p')
	for i in range(len(p_elems)):
		p_elem = p_elems[i]
		if 'Position:' in p_elem.text_content():
			post_position = p_elem.text_content().split(':')
			return post_position[1].split('\n')[0].strip()
	return None


def add_player_info(player_id_num, player_name, postion, url):
	pi_cursor = conn.cursor()
	statement = '''INSERT INTO player_info (PLAYER_ID, PLAYER_NAME, POSITION, PLAYER_URL) VALUES (?,?,?,?)'''
	values = [player_id_num, player_name, postion, url]
	pi_cursor.execute(statement, values)
	conn.commit()


def add_passing_row(game_dict, add_player):
	global PLAYER_ID_NUM, GAME_ID_NUM
	added_player = add_player
	increment_next = True
	if 'pass_att' in game_dict and game_dict['pass_att']:
		if not added_player:
			added_player = True
			PLAYER_ID_NUM += 1
		GAME_ID_NUM += 1
		increment_next = False
		game_dict['game_id'] = GAME_ID_NUM
		game_dict['player_id'] = PLAYER_ID_NUM

		key_list = list(game_dict.keys())
		values = [game_dict[key] for key in key_list]
		statement = '''INSERT INTO passing_statistics (%s) VALUES (%s)''' % (','.join([ID2COL[key] for key in key_list]), ','.join(['?']*len(key_list)))
		new_cursor = conn.cursor()
		new_cursor.execute(statement, values)
	return added_player, increment_next


def add_rushing_row(game_dict, add_player, increment_game_id):
	global PLAYER_ID_NUM, GAME_ID_NUM
	added_player = add_player
	increment_next = increment_game_id
	if 'rush_att' in game_dict and game_dict['rush_att']:
		if not added_player:
			added_player = True
			PLAYER_ID_NUM += 1

		if increment_next:
			GAME_ID_NUM += 1
			increment_next = False
		game_dict['game_id'] = GAME_ID_NUM
		game_dict['player_id'] = PLAYER_ID_NUM

		key_list = list(game_dict.keys())
		values = [game_dict[key] for key in key_list]
		statement = '''INSERT INTO rushing_statistics (%s) VALUES (%s)''' % (','.join([ID2COL[key] for key in key_list]), ','.join(['?']*len(key_list)))
		new_cursor = conn.cursor()
		new_cursor.execute(statement, values)
	return added_player, increment_next


def add_receiving_row(game_dict, add_player, increment_game_id):
	global PLAYER_ID_NUM, GAME_ID_NUM
	added_player = add_player
	if 'targets' in game_dict and game_dict['targets']:
		if not added_player:
			added_player = True
			PLAYER_ID_NUM += 1
		if increment_game_id:
			GAME_ID_NUM += 1
		game_dict['game_id'] = GAME_ID_NUM
		game_dict['player_id'] = PLAYER_ID_NUM

		key_list = list(game_dict.keys())
		values = [game_dict[key] for key in key_list]
		statement = '''INSERT INTO receiving_statistics (%s) VALUES (%s)''' % (','.join([ID2COL[key] for key in key_list]), ','.join(['?']*len(key_list)))
		new_cursor = conn.cursor()
		new_cursor.execute(statement, values)
	return added_player


def scrapePlayerGameLogs(url):
	global GAME_ID_NUM, PLAYER_ID_NUM
	add_player = False
	page = requests.get(url)
	pageText = page.text
	doc = lh.fromstring(pageText)
	pos = get_player_position(doc)
	print(pos)
	player_name = get_player_name(doc)
	player_postion = get_player_position(doc)
	tr_elements = doc.xpath('//tr')
	lengths = [len(t) for t in tr_elements]
	for i in range(len(tr_elements)):
		tr_elem = tr_elements[i]
		if isStatRow(tr_elem.items()):
			passing_game_dict = {'game_id': None, 'name': player_name, 'player_id': None, 'playoffs': isPlayoffRow(tr_elem.items()), 'position': player_postion}
			rushing_game_dict = {'game_id': None, 'name': player_name, 'player_id': None, 'playoffs': isPlayoffRow(tr_elem.items()), 'position': player_postion}
			receiving_game_dict = {'game_id': None, 'name': player_name, 'player_id': None, 'playoffs': isPlayoffRow(tr_elem.items()), 'position': player_postion}
			for c in tr_elem:
				addStatToDict(c.items(), c.text_content(), passing_game_dict, 'passing')
				addStatToDict(c.items(), c.text_content(), rushing_game_dict, 'rushing')
				addStatToDict(c.items(), c.text_content(), receiving_game_dict, 'receiving')

			add_player, increment_game_id = add_passing_row(passing_game_dict, add_player)
			add_player, increment_game_id = add_rushing_row(rushing_game_dict, add_player, increment_game_id)
			add_player = add_receiving_row(receiving_game_dict, add_player, increment_game_id)

	conn.commit()
	if add_player:
		add_player_info(PLAYER_ID_NUM, player_name, player_postion, url)


def scrape_player_list(url_list):
	for url in url_list:
		scrapePlayerGameLogs(url)


# ==== Year Scraping Functions =====
def get_existing_url_list():
	existing_url_list = []
	statement = '''SELECT DISTINCT PLAYER_URL from player_info'''
	cursor = conn.cursor()
	url_cursor = cursor.execute(statement)
	for url in url_cursor:
		existing_url_list.append(url[0])
	return existing_url_list


def get_url_list_from_year(year_url, existing_url_list):
	player_url_list = []
	page = requests.get(year_url)
	pageText = page.text
	doc = lh.fromstring(pageText)
	player_name = get_player_name(doc)
	tr_elements = doc.xpath('//tr')
	for i in range(len(tr_elements)):
		tr_elem = tr_elements[i]
		if len(tr_elem.items()) == 0:
			link_elem = tr_elem.getchildren()[1]
			try:
				link = link_elem.getchildren()[0]
				player_url = 'https://www.pro-football-reference.com' + link.items()[0][1][:-4] + '/gamelog/'
				if player_url not in existing_url_list and player_url not in player_url_list:
					player_url_list.append(player_url)
			except IndexError:
				continue
	return player_url_list


def get_url_list_for_years(year_list):
	year_url_list = []
	for year in year_list:
		year_url_passing = 'https://www.pro-football-reference.com/years/' + str(year) + '/passing.htm'
		year_url_rushing = 'https://www.pro-football-reference.com/years/' + str(year) + '/rushing.htm'
		year_url_receiving = 'https://www.pro-football-reference.com/years/' + str(year) + '/receiving.htm'
		year_url_list.extend([year_url_passing, year_url_rushing, year_url_receiving])
	return year_url_list


if __name__ == '__main__':
	initialize_values(drop_tables=True)
	# year_list = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
	year_list = [2018]
	year_url_list = get_url_list_for_years(year_list)
	for year in year_url_list:
		print(year)
		existing_url_list = get_existing_url_list()
		url_list = get_url_list_from_year(year, existing_url_list)
		print(len(url_list))
		scrape_player_list(url_list)

