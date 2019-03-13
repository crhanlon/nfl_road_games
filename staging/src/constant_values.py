# ==== General Stat IDs =====
def get_general_stat_ids():
	return ['game_id', 'year_id', 'game_date', 'game_num', 'age', 'team', 'position', 'game_location', 'opp', 'game_result', 'gs']


def get_type_dict():
	return {
	'game_id': str,
	'player_id': str,
	'name': str,
	'playoffs': int,
	'year_id': int,
	'game_date': str,
	'game_num': int,
	'age': float,
	'team': str,
	'position': str,
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
	'pass_adj_yds_per_att': float,
	'rush_att': int,
	'rush_yds': int,
	'rush_yds_per_att': float,
	'rush_td': int,
	'fumbles': int,
	'targets': int, 
	'rec': int, 
	'rec_yds': int, 
	'rec_yds_per_rec': float, 
	'rec_td': int, 
	'catch_pct': str, 
	'rec_yds_per_tgt': float
	}


def id_2_col():
	return {
	'game_id': 'GAME_ID',
	'player_id': 'PLAYER_ID',
	'name': 'NAME',
	'playoffs': 'PLAYOFFS',
	'year_id': 'YEAR',
	'game_date': 'DATE',
	'game_num': 'GAME_NUM',
	'age': 'AGE',
	'team': 'TEAM',
	'position': 'POSITION',
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
	'pass_adj_yds_per_att': 'PASS_ADJ_YARDS_PER_ATTEMPT',
	'rush_att': 'RUSH_ATTEMPTS',
	'rush_yds': 'RUSH_YARDS',
	'rush_yds_per_att': 'RUSH_YARDS_PER_ATT',
	'rush_td': 'RUSH_TD',
	'fumbles': 'FUMBLES',
	'targets': 'TARGETS',
	'rec': 'RECEPTIONS',
	'rec_yds': 'RECEIVING_YARDS',
	'rec_yds_per_rec': 'RECEIVING_YARDS_PER_REC',
	'rec_td': 'RECEIVING_TD',
	'catch_pct': 'CATCH_PCT',
	'rec_yds_per_tgt': 'RECEIVING_YARDS_PER_TARGET'
	}


# ==== QB Statistics =====
def get_qb_sql_cols():
	return [
	'GAME_ID',
	'PLAYER_ID',
	'NAME',
	'AGE',
	'PLAYOFFS',
	'YEAR',
	'DATE',
	'GAME_NUM',
	'TEAM',
	'POSITION',
	'GAME_LOCATION',
	'OPPONENT',
	'RESULT',
	'STARTED',
	'PASS_COMPLETIONS',
	'PASS_ATTEMPTS',
	'PASS_COMP_PCT',
	'PASS_YARDS',
	'PASS_TD',
	'PASS_INT',
	'PASS_RATING',
	'PASS_SACKED',
	'PASS_SACKED_YARDS',
	'PASS_YARDS_PER_ATT',
	'PASS_ADJ_YARDS_PER_ATTEMPT'
	]

def get_passing_stat_ids():
	return ['pass_cmp', 'pass_att', 'pass_cmp_perc', 'pass_yds', 'pass_td', 'pass_int', 'pass_rating', 'pass_sacked', 'pass_sacked_yds', 'pass_yds_per_att', 'pass_adj_yds_per_att']


def create_passing_table(conn):
	cursor = conn.cursor()
	cursor.execute('''CREATE TABLE IF NOT EXISTS passing_statistics
	         (GAME_ID 	INT 	PRIMARY KEY,
	         PLAYER_ID 	INT,
	         NAME 	TEXT,
	         AGE 	REAL,
	         PLAYOFFS 	INT,
	         YEAR 	INT,
	         DATE 	TEXT,
	         GAME_NUM 	INT,
	         TEAM 	TEXT,
	         POSITION 	TEXT,
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


# ==== Rushing Stats =====
def get_rushing_stat_ids():
	return ['rush_att', 'rush_yds', 'rush_yds_per_att', 'rush_td', 'fumbles']

def get_rushing_sql_cols():
	return [
	'GAME_ID',
	'PLAYER_ID',
	'NAME',
	'AGE',
	'PLAYOFFS',
	'YEAR',
	'DATE',
	'GAME_NUM',
	'TEAM',
	'POSITION',
	'GAME_LOCATION',
	'OPPONENT',
	'RESULT',
	'STARTED',
	'RUSH_ATTEMPTS',
	'RUSH_YARDS',
	'RUSH_YARDS_PER_ATT',
	'RUSH_TD',
	'FUMBLES'
	]


def create_rushing_table(conn):
	cursor = conn.cursor()
	cursor.execute('''CREATE TABLE IF NOT EXISTS rushing_statistics
	         (GAME_ID 	INT 	PRIMARY KEY,
	         PLAYER_ID 	INT,
	         NAME 	TEXT,
	         AGE 	REAL,
	         PLAYOFFS 	INT,
	         YEAR 	INT,
	         DATE 	TEXT,
	         GAME_NUM 	INT,
	         TEAM 	TEXT,
	         POSITION 	TEXT,
	         GAME_LOCATION 	TEXT,
	         OPPONENT 	TEXT,
	         RESULT 	TEXT,
	         STARTED 	TEXT,
	         RUSH_ATTEMPTS 	INT,
	         RUSH_YARDS 	INT,
	         RUSH_YARDS_PER_ATT 	REAL,
	         RUSH_TD 	INT,
	         FUMBLES 	INT);''')
	conn.commit()
	print('Created Rushing Table')


# ==== Receiving Stats =====
def get_receiving_stat_ids():
	return ['targets', 'rec', 'rec_yds', 'rec_yds_per_rec', 'rec_td', 'catch_pct', 'rec_yds_per_tgt']

def get_receiving_sql_cols():
	return [
	'GAME_ID',
	'PLAYER_ID',
	'NAME',
	'AGE',
	'PLAYOFFS',
	'YEAR',
	'DATE',
	'GAME_NUM',
	'TEAM',
	'POSITION',
	'GAME_LOCATION',
	'OPPONENT',
	'RESULT',
	'STARTED',
	'TARGETS',
	'RECEPTIONS',
	'RECEIVING_YARDS',
	'RECEIVING_YARDS_PER_REC',
	'RECEIVING_TD',
	'CATCH_PCT',
	'RECEIVING_YARDS_PER_TARGET'
	]


def create_receiving_table(conn):
	cursor = conn.cursor()
	cursor.execute('''CREATE TABLE IF NOT EXISTS receiving_statistics
	         (GAME_ID 	INT 	PRIMARY KEY,
	         PLAYER_ID 	INT,
	         NAME 	TEXT,
	         AGE 	REAL,
	         PLAYOFFS 	INT,
	         YEAR 	INT,
	         DATE 	TEXT,
	         GAME_NUM 	INT,
	         TEAM 	TEXT,
	         POSITION 	TEXT,
	         GAME_LOCATION 	TEXT,
	         OPPONENT 	TEXT,
	         RESULT 	TEXT,
	         STARTED 	TEXT,
	         TARGETS 	INT,
	         RECEPTIONS 	INT,
	         RECEIVING_YARDS 	INT,
	         RECEIVING_YARDS_PER_REC 	REAL,
	         RECEIVING_TD 	INT,
	         CATCH_PCT 	TEXT,
	         RECEIVING_YARDS_PER_TARGET 	REAL);''')
	conn.commit()
	print('Created Rushing Table')


# ==== MISC
def operation_2_str():
	return {
	'>': 'greater',
	'>=': 'greaterEq',
	'<': 'less',
	'<=': 'lessEq',
	'=': 'equal'
	}


def year_2_idx():
	d = {}
	for i in range(2, 13):
		d[str(i + 2006)] = i
	d['all'] = 13
	return d



