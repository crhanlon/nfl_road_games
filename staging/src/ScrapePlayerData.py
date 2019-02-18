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
DATABASE_FILENAME = 'qb_statistics.db'
DATABASE_PATH = os.path.join(DATA_DIR, DATABASE_FILENAME)
conn = sqlite3.connect(DATABASE_PATH)
print('OPENED CONNECTION')


# ==== Test SQL Connection =====
def create_table():
	CURSOR = conn.cursor()
	CURSOR.execute('''DROP TABLE qb_statistics''')
	CURSOR.execute('''CREATE TABLE qb_statistics
	         (GAME_ID 	TEXT 	PRIMARY KEY,
	         PLAYER_ID 	TEXT,
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
	print('Created Table')

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

# ==== ID Counters =====
GAME_ID_NUM = -1


def getColumns():
	columns = ['name', 'player_id', 'playoffs']
	columns.extend(GENERAL_STAT_IDS)
	columns.extend(PASSING_STAT_IDS)
	return columns


def initializeDF():
	df = pd.DataFrame(columns=getColumns())
	return df


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


def scrapePlayerGameLogs(url, df):
	global GAME_ID_NUM
	page = requests.get(url)
	pageText = page.text
	doc = lh.fromstring(pageText)
	tr_elements = doc.xpath('//tr')
	lengths = [len(t) for t in tr_elements]
	for i in range(len(tr_elements)):
		tr_elem = tr_elements[i]
		if isStatRow(tr_elem.items()):
			GAME_ID_NUM += 1
			game_dict = {'game_id': 'game' + str(GAME_ID_NUM), 'name': 'Phillip Rivers', 'player_id': 'player000000', 'playoffs': isPlayoffRow(tr_elem.items())}
			for c in tr_elem:
				addStatToDict(c.items(), c.text_content(), game_dict)
			key_list = list(game_dict.keys())
			values = [game_dict[key] for key in key_list]
			statement = '''INSERT INTO qb_statistics (%s) VALUES (%s)''' % (','.join([ID2COL[key] for key in key_list]), ','.join(['?']*len(key_list)))
			print(statement, values)
			new_cursor = conn.cursor()
			new_cursor.execute(statement, values)
			print(new_cursor.lastrowid)
			df = df.append(game_dict, ignore_index=True)
	conn.commit()
	return df



if __name__ == '__main__':
	df = initializeDF()
	create_table()
	df = scrapePlayerGameLogs('https://www.pro-football-reference.com/players/R/RivePh00/gamelog/', df)
	# print(df)
	# df.to_csv(OUTPUT_FILE, mode='w')
	# df.to_csv(OUTPUT_FILE, mode='a', header=False)
