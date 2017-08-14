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
    
def create_teamGameDict(gameID):
    teamGameDict = {}
    teamGameDict['gameID'] = gameID
    teamGameDict['home_team_stats'] = get_home_team_stats(gameID)
    teamGameDict['away_team_stats'] = get_away_team_stats(gameID)
    teamGameDict['game_date'] = get_game_date(gameID)
    teamGameDict['home_team'] = get_home_team(gameID)
    teamGameDict['away_team'] = get_away_team(gameID)
    teamGameDict['odds_details'] = get_odds_details(gameID)
    
    return teamGameDict    

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


    gameIDs = create_gameIDList('20111225')

    pool = multiprocessing.Pool(8)
    teamGameList = []
    for x in pool.map(create_teamGameDict, gameIDs):
        teamGameList.append(x)

    pool.terminate()
    pool.join()

    for teamGame in teamGameList:
        print('Processing game.. '+ teamGame['gameID'])        
        values = [teamGame['gameID'], str(teamGame['game_date']),
                    teamGame['home_team']['name'],
                    teamGame['home_team']['abbrev'],
                    teamGame['away_team']['name'],
                    teamGame['away_team']['abbrev'],
                    teamGame['home_team_stats']['fga'],
                    teamGame['home_team_stats']['fgm'],
                    teamGame['home_team_stats']['3pta'],
                    teamGame['home_team_stats']['3ptm'],
                    teamGame['home_team_stats']['fta'],
                    teamGame['home_team_stats']['ftm'],
                    teamGame['home_team_stats']['oreb'],
                    teamGame['home_team_stats']['dreb'],
                    teamGame['home_team_stats']['ast'],
                    teamGame['home_team_stats']['stl'],
                    teamGame['home_team_stats']['blk'],
                    teamGame['home_team_stats']['to'],
                    teamGame['home_team_stats']['pf'],
                    teamGame['home_team_stats']['pts'],
                    teamGame['home_team_stats']['fastBreakPts'],
                    teamGame['home_team_stats']['ptsInPaint'],
                    teamGame['home_team_stats']['technicalFouls'],
                    teamGame['home_team_stats']['flagrantFouls'],
                    teamGame['away_team_stats']['fga'],
                    teamGame['away_team_stats']['fgm'],
                    teamGame['away_team_stats']['3pta'],
                    teamGame['away_team_stats']['3ptm'],
                    teamGame['away_team_stats']['fta'],
                    teamGame['away_team_stats']['ftm'],
                    teamGame['away_team_stats']['oreb'],
                    teamGame['away_team_stats']['dreb'],
                    teamGame['away_team_stats']['ast'],
                    teamGame['away_team_stats']['stl'],
                    teamGame['away_team_stats']['blk'],
                    teamGame['away_team_stats']['to'],
                    teamGame['away_team_stats']['pf'],
                    teamGame['away_team_stats']['pts'],
                    teamGame['away_team_stats']['fastBreakPts'],
                    teamGame['away_team_stats']['ptsInPaint'],
                    teamGame['away_team_stats']['technicalFouls'],
                    teamGame['away_team_stats']['flagrantFouls'],
                    teamGame['odds_details']['line'],
                    teamGame['odds_details']['line'],
                    teamGame['odds_details']['over/under']]
 
        #cursor.execute(SQLCommand, values)
        #connection.commit()
        #print('Import for game ' + ID + ' committed')

    print('End time: ' + str(datetime.datetime.time(datetime.datetime.now()))) 
    connection.close()   


