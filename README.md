# nfl_road_games

## Basic Info
Data ingestion and processing scripts are in python3.  Data is stored in a SQL database.


## Setup
Run ```git clone https://github.com/crhanlon/nfl_road_games.git```

Navigate to the staging/src folder
Run ```./setup_data.sh```

Now the data folder is set up, with the database initialized.  Running
```python3 ScrapePlayerData.py``` will ingest the career gamelogs for all players who threw a pass in from 2008-2018.  This can be changed in the main function of the file.
