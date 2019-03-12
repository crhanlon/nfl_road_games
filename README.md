# nfl_road_games

## Basic Info
Data ingestion and processing scripts are in python3.  Data is stored in a SQL database.


## Setup
Run ```git clone https://github.com/crhanlon/nfl_road_games.git```

Navigate to the staging/src folder
Run ```./setup_data.sh```

Now the data folder is set up, with the database initialized.  Running
```python3 ScrapePlayerData.py``` will ingest the career gamelogs for all players who had a pass attempt, rush attempt, or target in 2018.  Changing the year_list in the main function will lead to ingesting other years as well.

With the database set up, you can now analyze the results.  In the analyze_player_data.py function, there is a section to change queries.

```# ==== Organize Inputs =====
# CHANGE HERE
AUTOMATICALLY_OVERWRITE = False
DATA_TYPE = 'passing'

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
  ```
The AUTOMATICALLY_OVERWRITE variable will overwrite any existing file with the same parameters if set to True, and will raise an exception if set to False.

The DATA_TYPE variable sets what table you are querying.  ```'passing'``` will query passing_statistics, ```'rushing'``` will query rushing_statistics, and ```'receiving'``` will query receiving_statistics.

The get_key_value_dict() function serves to return a dictionary with your desired SQL query parameters (using the DATA_TYPE variable as well.  The above settings will give the SQL command

```SELECT * FROM passing_statistics WHERE PASS_ATTEMPTS >= 10 AND YEAR = 2018;```
