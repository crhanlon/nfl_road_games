

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

def get_general_stat_ids():
	return ['game_id', 'year_id', 'game_date', 'game_num', 'age', 'team', 'game_location', 'opp', 'game_result', 'gs']

def get_passing_stat_ids():
	return ['pass_cmp', 'pass_att', 'pass_cmp_perc', 'pass_yds', 'pass_td', 'pass_int', 'pass_rating', 'pass_sacked', 'pass_sacked_yds', 'pass_yds_per_att', 'pass_adj_yds_per_att']

def get_qb_stat_ids_to_sql():
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

def qb_stat_ids_to_type():
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



