'''
Module that 
'''
import nba_games
import nba_teams
import nba_games
import pypyodbc
import datetime
import time


print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))

connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

SQLCommand = ('INSERT INTO dbo.PlayerGameStats(playerID, gameID, playerGameID, gameDate, teamID,'+
              'playerName, min, fga, fgm, [3pta], [3ptm], fta, ftm, oreb, dreb, ast, stl, blk, [to], pf,'+
              'plusminus, pts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')

year = '2013'
months = ['october','november','december','january','february','march','april']
month = 'november'

gameIDList = nba_games.create_refIDList(month, year)
game_dict = nba_games.create_player_game_dict(gameIDList)
print('Player game dicts created for %s'%month + '%s'%year)
    
for gameID in game_dict:
    for playerID in game_dict[gameID]['playerStats']:
        values = [playerID, gameID, gameID+playerID, game_dict[gameID]['gameDate'],
                            game_dict[gameID]['playerStats'][playerID]['teamID'],
                            game_dict[gameID]['playerStats'][playerID]['playerName'],
                            game_dict[gameID]['playerStats'][playerID]['mp'],
                            game_dict[gameID]['playerStats'][playerID]['fga'],
                            game_dict[gameID]['playerStats'][playerID]['fg'],
                            game_dict[gameID]['playerStats'][playerID]['fg3a'],
                            game_dict[gameID]['playerStats'][playerID]['fg3'],
                            game_dict[gameID]['playerStats'][playerID]['fta'],
                            game_dict[gameID]['playerStats'][playerID]['ft'],
                            game_dict[gameID]['playerStats'][playerID]['orb'],
                            game_dict[gameID]['playerStats'][playerID]['drb'],
                            game_dict[gameID]['playerStats'][playerID]['ast'],
                            game_dict[gameID]['playerStats'][playerID]['stl'],
                            game_dict[gameID]['playerStats'][playerID]['blk'],
                            game_dict[gameID]['playerStats'][playerID]['tov'],
                            game_dict[gameID]['playerStats'][playerID]['pf'],
                            game_dict[gameID]['playerStats'][playerID]['plus_minus'],
                            game_dict[gameID]['playerStats'][playerID]['pts']]    
        cursor.execute(SQLCommand, values)
        connection.commit()
        print('Import for player ' + playerID + 'in game '+ gameID + 'committed.')

print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
connection.close()

