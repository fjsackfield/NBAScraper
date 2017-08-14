import nba_games
import nba_teams
import pypyodbc
import datetime

connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()



IDList = nba_games.create_IDList('20161101')


game_dict = nba_games.create_nba_game_dict(['400899375'])


'''
for key in game_dict:
    update_list = [game_dict[key]['game_date'],
              game_dict[key]['home_box_score']['TEAM']['fastBreakPts'],
              game_dict[key]['home_box_score']['TEAM']['ptsInPaint'],
              game_dict[key]['home_box_score']['TEAM']['technicalFouls'],
              game_dict[key]['home_box_score']['TEAM']['flagrantFouls'],
              game_dict[key]['away_box_score']['TEAM']['fastBreakPts'],
              game_dict[key]['away_box_score']['TEAM']['ptsInPaint'],
              game_dict[key]['away_box_score']['TEAM']['technicalFouls'],
              game_dict[key]['away_box_score']['TEAM']['flagrantFouls'],
              key]
    date = "'" + str(update_list[0]) + "'"
    SQLCommand = ('UPDATE dbo.TeamGameStats SET ' +
              'gameDate = %s,'%date +
              '[homeTeam.fastBreakPts] = %s,' %update_list[1] +
              '[homeTeam.ptsInPaint] = %s,'%update_list[2] +
              '[homeTeam.technicalFouls] = %s,'%update_list[3] +
              '[homeTeam.flagrantFouls] = %s,'%update_list[4] +
              '[awayTeam.fastBreakPts] = %s,'%update_list[5] +
              '[awayTeam.ptsInPaint] = %s,'%update_list[6] +
              '[awayTeam.technicalFouls] = %s,'%update_list[7] +
              '[awayTeam.flagrantFouls] = %s'%update_list[8] +
              'WHERE gameID = %s'%update_list[9])
    cursor.execute(SQLCommand)
    connection.commit()
    print('Update for game ' + key + ' committed.')
'''
for key in game_dict:
    update_list = [game_dict[key]['game_date'],key]
    date = "'" + str(update_list[0]) + "'"
    print(date)
    print(len(date))
    
    SQLCommand = ('UPDATE dbo.TeamGameStats SET ' +
              'gameDate = %s'%date +
              'WHERE gameID = %s'%update_list[1])
    cursor.execute(SQLCommand)
    connection.commit()
    
    print('Update for game ' + key + ' committed.')




connection.close()

              
              
