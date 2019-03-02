# nfl_road_games

## Basic Info
Data ingestion and processing scripts are in python3.  Data is stored in a SQL database.


## Setup
Run ```git clone https://github.com/crhanlon/nfl_road_games.git```

Navigate to the staging/src folder
Run ```./setup_data.sh```

Now the data folder is set up, with the database initialized.  Running
```python3 ScrapePlayerData.py``` will ingest the career gamelogs for all players who had a pass attempt, rush attempt, or target in 2018.  Changing the year_list in the main function will lead to ingesting other years as well.
