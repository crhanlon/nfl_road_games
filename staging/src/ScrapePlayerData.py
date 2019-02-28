import requests
import lxml.html as lh
import pandas as pd
import numpy as np
import os
import sqlite3


# ==== Output Files =====
CURRENT_FILE_DIR = 'os.path.dirname(os.path.realpath(__file__))'
DATA_DIR = '../../data'
FILENAME = 'qb_statistics.csv'
OUTPUT_FILE = os.path.join(DATA_DIR, FILENAME)
DATABASE_FILENAME = 'nfl_road_statistics.db'
DATABASE_PATH = os.path.join(DATA_DIR, DATABASE_FILENAME)
conn = sqlite3.connect(DATABASE_PATH)
print('OPENED CONNECTION')

# temp_cursor = conn.cursor()
# temp_cursor.execute('''DROP TABLE IF EXISTS qb_statistics''')


# ==== Initialize Table Setup =====
def create_qb_table():
	cursor = conn.cursor()
	cursor.execute('''CREATE TABLE IF NOT EXISTS qb_statistics
	         (GAME_ID 	INT 	PRIMARY KEY,
	         PLAYER_ID 	INT,
	         NAME 	TEXT,
	         AGE 	REAL,
	         PLAYOFFS 	INT,
	         YEAR 	INT,
	         DATE 	TEXT,
	         GAME_NUM 	INT,
	         TEAM 	TEXT,
	         GAME_LOCATION 	TEXT,
	         OPPONENT 	TEXT,
	         RESULT 	TEXT,
	         STARTED 	TEXT,
	         PASS_COMPLETIONS 	INT,
	         PASS_ATTEMPTS 	INT,
	         PASS_COMP_PCT 	REAL,
	         PASS_YARDS 	INT,
	         PASS_TD 	INT,
	         PASS_INT  	INT,
	         PASS_RATING 	REAL,
	         PASS_SACKED 	INT,
	         PASS_SACKED_YARDS 	INT,
	         PASS_YARDS_PER_ATT 	REAL,
	         PASS_ADJ_YARDS_PER_ATTEMPT 	REAL);''')
	conn.commit()
	print('Created QB Table')


def create_player_table():
	pc_cursor = conn.cursor()
	pc_cursor.execute('''CREATE TABLE IF NOT EXISTS player_info
		(PLAYER_ID INT PRIMARY_KEY,
		PLAYER_NAME TEXT,
		PLAYER_URL TEXT);''')
	conn.commit()
	print('Created Player Table')


def drop_all_tables():
	drop_cursor = conn.cursor()
	drop_cursor.execute('''DROP TABLE IF EXISTS qb_statistics''')
	drop_cursor.execute('''DROP TABLE IF EXISTS player_info''')


# ==== Constants =====
DB_STAT_ORDER = [ 'GAME_ID', 'PLAYER_ID', 'NAME', 'AGE', 'PLAYOFFS', 'YEAR', 'DATE', 'GAME_NUM', 'TEAM', 'HOME', 'OPPONENT',
			'RESULT', 'STARTED', 'PASS_COMPLETIONS', 'PASS_ATTEMPTS', 'PASS_COMP_PCT', 'PASS_YARDS',
         	'PASS_TD', 'PASS_INT', 'PASS_RATING', 'PASS_SACKED', 'PASS_SACKED_YARDS',
         	'PASS_YARDS_PER_ATT', 'PASS_ADJ_YARDS_PER_ATTEMPT' ]
GENERAL_STAT_IDS = ['game_id', 'year_id', 'game_date', 'game_num', 'age', 'team', 'game_location', 'opp', 'game_result', 'gs']
PASSING_STAT_IDS = ['pass_cmp', 'pass_att', 'pass_cmp_perc', 'pass_yds', 'pass_td', 'pass_int', 'pass_rating', 'pass_sacked', 'pass_sacked_yds', 'pass_yds_per_att', 'pass_adj_yds_per_att']

ID2COL = {
	'game_id': 'GAME_ID',
	'player_id': 'PLAYER_ID',
	'name': 'NAME',
	'playoffs': 'PLAYOFFS',
	'year_id': 'YEAR',
	'game_date': 'DATE',
	'game_num': 'GAME_NUM',
	'age': 'AGE',
	'team': 'TEAM',
	'game_location': 'GAME_LOCATION',
	'opp': 'OPPONENT',
	'game_result': 'RESULT',
	'gs': 'STARTED',
	'pass_cmp': 'PASS_COMPLETIONS',
	'pass_att': 'PASS_ATTEMPTS',
	'pass_cmp_perc': 'PASS_COMP_PCT',
	'pass_yds': 'PASS_YARDS',
	'pass_td': 'PASS_TD',
	'pass_int': 'PASS_INT',
	'pass_rating': 'PASS_RATING',
	'pass_sacked': 'PASS_SACKED',
	'pass_sacked_yds': 'PASS_SACKED_YARDS',
	'pass_yds_per_att': 'PASS_YARDS_PER_ATT',
	'pass_adj_yds_per_att': 'PASS_ADJ_YARDS_PER_ATTEMPT'
}

TYPE_DICT = {
	'game_id': str,
	'player_id': str,
	'name': str,
	'playoffs': int,
	'year_id': int,
	'game_date': str,
	'game_num': int,
	'age': float,
	'team': str,
	'game_location': str,
	'opp': str,
	'game_result': str,
	'gs': str,
	'pass_cmp': int,
	'pass_att': int,
	'pass_cmp_perc': float,
	'pass_yds': int,
	'pass_td': int,
	'pass_int': int,
	'pass_rating': float,
	'pass_sacked': int,
	'pass_sacked_yds': int,
	'pass_yds_per_att': float,
	'pass_adj_yds_per_att': float
}

GAME_ID_NUM = -1
PLAYER_ID_NUM = -1


def update_ids():
	global GAME_ID_NUM, PLAYER_ID_NUM
	id_cursor = conn.cursor()
	new_game_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(GAME_ID) FROM qb_statistics''')][0]
	if new_game_id != None:
		GAME_ID_NUM = new_game_id

	new_player_id = [cid[0] for cid in id_cursor.execute('''SELECT MAX(PLAYER_ID) FROM qb_statistics''')][0]
	if new_player_id != None:
		PLAYER_ID_NUM = new_player_id


def initialize_values(drop_tables=False):
	if drop_tables:
		drop_all_tables()
	create_qb_table()
	create_player_table()
	update_ids()


def getColumns():
	columns = ['name', 'player_id', 'playoffs']
	columns.extend(GENERAL_STAT_IDS)
	columns.extend(PASSING_STAT_IDS)
	return columns


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


def addStatToDict(col_items, text, game_dict):
	global TYPE_DICT
	for item in col_items:
		if item[0] == 'data-stat' and item[1] in getColumns():
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


def add_player_info(player_id_num, player_name, url):
	pi_cursor = conn.cursor()
	statement = '''INSERT INTO player_info (PLAYER_ID, PLAYER_NAME, PLAYER_URL) VALUES (?,?,?)'''
	values = [player_id_num, player_name, url]
	pi_cursor.execute(statement, values)
	conn.commit()


def scrapePlayerGameLogs(url):
	global GAME_ID_NUM, PLAYER_ID_NUM
	add_player = False
	page = requests.get(url)
	pageText = page.text
	doc = lh.fromstring(pageText)
	player_name = get_player_name(doc)
	tr_elements = doc.xpath('//tr')
	lengths = [len(t) for t in tr_elements]
	for i in range(len(tr_elements)):
		tr_elem = tr_elements[i]
		if isStatRow(tr_elem.items()):
			game_dict = {'game_id': None, 'name': player_name, 'player_id': None, 'playoffs': isPlayoffRow(tr_elem.items())}
			for c in tr_elem:
				addStatToDict(c.items(), c.text_content(), game_dict)

			if 'pass_att' in game_dict and game_dict['pass_att']:
				if not add_player:
					add_player = True
					PLAYER_ID_NUM += 1
				GAME_ID_NUM += 1
				game_dict['game_id'] = GAME_ID_NUM
				game_dict['player_id'] = PLAYER_ID_NUM

				key_list = list(game_dict.keys())
				values = [game_dict[key] for key in key_list]
				statement = '''INSERT INTO qb_statistics (%s) VALUES (%s)''' % (','.join([ID2COL[key] for key in key_list]), ','.join(['?']*len(key_list)))
				new_cursor = conn.cursor()
				new_cursor.execute(statement, values)
				if int(new_cursor.lastrowid) % 100 == 0:
					print(new_cursor.lastrowid)
	conn.commit()
	if add_player:
		add_player_info(PLAYER_ID_NUM, player_name, url)


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
				if player_url not in existing_url_list:
					player_url_list.append(player_url)
			except IndexError:
				continue
	return player_url_list


def get_url_list_for_years(year_list):
	year_url_list = []
	for year in year_list:
		year_url = 'https://www.pro-football-reference.com/years/' + str(year) + '/passing.htm'
		year_url_list.append(year_url)
	return year_url_list


if __name__ == '__main__':
	initialize_values(drop_tables=True)
	year_list = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
	year_url_list = get_url_list_for_years(year_list)
	for year in year_url_list:
		print(year)
		existing_url_list = get_existing_url_list()
		url_list = get_url_list_from_year(year, existing_url_list)
		print(len(url_list))
		scrape_player_list(url_list)
