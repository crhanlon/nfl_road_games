import requests
import lxml.html as lh
import pandas as pd
import numpy as np
import os


# ==== Output Files =====
DATA_DIR = '../../data'
FILENAME = 'qb_statistics.csv'
OUTPUT_FILE = os.path.join(DATA_DIR, FILENAME)

# ==== Constants =====
GENERAL_STAT_IDS = ['year_id', 'game_date', 'game_num', 'age', 'team', 'game_location', 'opp', 'game_result', 'gs']
PASSING_STAT_IDS = ['pass_cmp', 'pass_att', 'pass_cmp_perc', 'pass_yds', 'pass_td', 'pass_int', 'pass_rating', 'pass_sacked', 'pass_sacked_yds', 'pass_yds_per_att', 'pass_adj_yds_per_att']

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
	return isPRow


def addStatToDict(col_items, text, gameDict):
	for item in col_items:
		if item[0] == 'data-stat' and item[1] in getColumns():
			gameDict[item[1]] = text


def scrapePlayerGameLogs(url, df):
	page = requests.get(url)
	pageText = page.text
	doc = lh.fromstring(pageText)
	tr_elements = doc.xpath('//tr')
	lengths = [len(t) for t in tr_elements]
	for i in range(len(tr_elements)):
		tr_elem = tr_elements[i]
		if isStatRow(tr_elem.items()):
			gameDict = {'name': 'Phillip Rivers', 'player_id': 'player000000', 'playoffs': isPlayoffRow(tr_elem.items())}
			for c in tr_elem:
				addStatToDict(c.items(), c.text_content(), gameDict)
			df = df.append(gameDict, ignore_index=True)
	return df



if __name__ == '__main__':
	df = initializeDF()
	df = scrapePlayerGameLogs('https://www.pro-football-reference.com/players/R/RivePh00/gamelog/', df)
	print(df)
	# df.to_csv(OUTPUT_DIR, mode='w')
	df.to_csv(OUTPUT_FILE, mode='a', header=False)

