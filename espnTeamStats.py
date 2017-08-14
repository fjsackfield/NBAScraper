from nba_boxscore_scraper import get_game_date
from nba_boxscore_scraper import get_home_team
from nba_boxscore_scraper import get_away_team
from nba_boxscore_scraper import get_home_team_stats
from nba_boxscore_scraper import get_away_team_stats
from nba_boxscore_scraper import get_odds_details
from nba_games import create_gameIDList
import multiprocessing
from functools import partial
import time
import datetime
import requests
from bs4 import BeautifulSoup
import sys
import pypyodbc
import re
    


if __name__ == "__main__":
    
    print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    

    connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
    cursor = connection.cursor()

    SQLCommand = ('INSERT INTO '+
                  'dbo.TeamGameStats(gameID, gameDate, '+
                  'homeTeamName, homeTeamAbbr, '+
                  'awayTeamName, awayTeamAbbr, '+
                  '[homeTeam.fga], [homeTeam.fgm], '+
                  '[homeTeam.3pta], [homeTeam.3ptm], '+
                  '[homeTeam.fta], [homeTeam.ftm], ' +
                  '[homeTeam.oreb], [homeTeam.dreb], '+
                  '[homeTeam.ast], [homeTeam.stl], '+
                  '[homeTeam.blk], [homeTeam.to], '+
                  '[homeTeam.pf], [homeTeam.pts],'+
                  '[homeTeam.fastBreakPts], [homeTeam.ptsInPaint], '+
                  '[homeTeam.technicalFouls], [homeTeam.flagrantFouls], '+
                  '[awayTeam.fga], [awayTeam.fgm], '+
                  '[awayTeam.3pta], [awayTeam.3ptm], '+
                  '[awayTeam.fta], [awayTeam.ftm], '+
                  '[awayTeam.oreb], [awayTeam.dreb], ' +
                  '[awayTeam.ast], [awayTeam.stl], '+
                  '[awayTeam.blk], [awayTeam.to], '+
                  '[awayTeam.pf], [awayTeam.pts], '+
                  '[awayTeam.fastBreakPts], [awayTeam.ptsInPaint], '+
                  '[awayTeam.technicalFouls], [awayTeam.flagrantFouls],' +
                  'favorite, line, [over/under]) VALUES' +
                  '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')


    gameIDs = create_gameIDList('20170302')

    for ID in gameIDs:
        print('Processing game.. '+ ID)
        
        home_team_stats = get_home_team_stats(ID)
        away_team_stats = get_away_team_stats(ID)
        game_date = get_game_date(ID)
        home_team = get_home_team(ID)
        away_team = get_away_team(ID)
        odds_details = get_odds_details(ID)

        
        values = [ID, str(game_date), home_team['name'], home_team['abbrev'],
                  away_team['name'], away_team['abbrev'],
                  home_team_stats['fga'], home_team_stats['fgm'],
                  home_team_stats['3pta'], home_team_stats['3ptm'],
                  home_team_stats['fta'], home_team_stats['ftm'],
                  home_team_stats['oreb'], home_team_stats['dreb'],
                  home_team_stats['ast'], home_team_stats['stl'],
                  home_team_stats['blk'], home_team_stats['to'],
                  home_team_stats['pf'], home_team_stats['pts'],
                  home_team_stats['fastBreakPts'],
                  home_team_stats['ptsInPaint'],
                  home_team_stats['technicalFouls'],
                  home_team_stats['flagrantFouls'],
                  away_team_stats['fga'], away_team_stats['fgm'],
                  away_team_stats['3pta'], away_team_stats['3ptm'],
                  away_team_stats['fta'], away_team_stats['ftm'],
                  away_team_stats['oreb'], away_team_stats['dreb'],
                  away_team_stats['ast'], away_team_stats['stl'],
                  away_team_stats['blk'], away_team_stats['to'],
                  away_team_stats['pf'], away_team_stats['pts'],
                  away_team_stats['fastBreakPts'],
                  away_team_stats['ptsInPaint'],
                  away_team_stats['technicalFouls'],
                  away_team_stats['flagrantFouls'],
                  odds_details['line']['favorite'],
                  odds_details['line']['margin'],
                  odds_details['over/under']]    
        #cursor.execute(SQLCommand, values)
        #connection.commit()
        #print('Import for game ' + ID + ' committed')

    print('End time: ' + str(datetime.datetime.time(datetime.datetime.now()))) 
    connection.close()   





    
