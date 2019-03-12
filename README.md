# nfl_road_games

## Basic Info
Data ingestion and processing scripts are in python3.  Data is stored in a SQL database.


## Setup
Run ```git clone https://github.com/crhanlon/nfl_road_games.git```

Navigate to the staging/src folder
Run ```./setup_data.sh```

Now the data folder is set up, with the database initialized.  Running
```python3 scrape_player_data.py``` will ingest the career gamelogs for all players who had a pass attempt, rush attempt, or target in 2018.  Changing the year_list in the main function will lead to ingesting other years as well.

With the database set up, you can now analyze the results.  In the analyze_player_data.py function, there is a section to change queries.

```python
# ==== Organize Inputs =====
# CHANGE HERE
AUTOMATICALLY_OVERWRITE = False
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
  ```
The AUTOMATICALLY_OVERWRITE variable will overwrite any existing file with the same parameters if set to True, and will raise an exception if set to False.

The DATA_TYPE variable sets what table you are querying.  ```'passing'``` will query passing_statistics, ```'rushing'``` will query rushing_statistics, and ```'receiving'``` will query receiving_statistics.

The get_key_value_dict_list() function serves to return a list of dictionaries, where each dictionary represents your desired SQL query parameters (using the DATA_TYPE variable as well).  The above settings will give the SQL commands

```sql
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR >= 2008;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2008;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2009;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2010;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2011;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2012;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2013;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2014;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2015;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2016;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2017;
SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2018;
```

Note: The YEAR variable is representative of the season, not the date.  So YEAR = 2018 will contain the Super Bowl that took place in 2019, not the Super Bowl that took place in 2018.

After running the script having set these values, you should now have the yearly data for all statistics from 2008-2018, as well as the totals over those years.
