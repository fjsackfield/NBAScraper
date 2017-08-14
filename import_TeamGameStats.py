import nba_games
import nba_teams
import pypyodbc
import datetime


print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
teams = nba_teams.create_nba_team_dict()

connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

SQLCommand = ('INSERT INTO dbo.TeamGameStats(gameID, gameDate, homeTeamName, homeTeamAbbr, homeTeamID,' +
'awayTeamName, awayTeamAbbr, awayTeamID,' +
'[homeTeam.fga], [homeTeam.fgm],' +
'[homeTeam.3pta], [homeTeam.3ptm], [homeTeam.fta], [homeTeam.ftm],' +
'[homeTeam.oreb], [homeTeam.dreb], [homeTeam.ast], [homeTeam.stl],' +
'[homeTeam.blk], [homeTeam.to], [homeTeam.pf],[homeTeam.pts],' +
'[homeTeam.fastBreakPts], [homeTeam.ptsInPaint], [homeTeam.technicalFouls],' +
'[homeTeam.flagrantFouls],' +
'[awayTeam.fga], [awayTeam.fgm], [awayTeam.3pta], [awayTeam.3ptm],' +
'[awayTeam.fta], [awayTeam.ftm], [awayTeam.oreb], [awayTeam.dreb],' +
'[awayTeam.ast], [awayTeam.stl], [awayTeam.blk], [awayTeam.to], [awayTeam.pf],' +
'[awayTeam.pts],' +
'[awayTeam.fastBreakPts], [awayTeam.ptsInPaint], [awayTeam.technicalFouls],' +
'[awayTeam.flagrantFouls],' +
'favorite, line, [over/under]) VALUES' +
'(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')



dateList = ['20151103']
for date in dateList:
    gameIDList = nba_games.create_IDList(date)
    game_dict = nba_games.create_nba_game_dict(gameIDList)
    print('gameIDList created for week of ' + date)
    
    for key in game_dict:
        game_date = str(game_dict[key]['game_date'])
        values = [key, game_date, game_dict[key]['home_team']['name'],
                    game_dict[key]['home_team']['abbrev'],
                    teams[game_dict[key]['home_team']['name']]['teamID'],
                    game_dict[key]['away_team']['name'],
                    game_dict[key]['away_team']['abbrev'],
                    teams[game_dict[key]['away_team']['name']]['teamID'],
                    game_dict[key]['home_box_score']['TEAM']['fga'],
                    game_dict[key]['home_box_score']['TEAM']['fgm'],
                    game_dict[key]['home_box_score']['TEAM']['3pta'],
                    game_dict[key]['home_box_score']['TEAM']['3ptm'],
                    game_dict[key]['home_box_score']['TEAM']['fta'],
                    game_dict[key]['home_box_score']['TEAM']['ftm'],
                    game_dict[key]['home_box_score']['TEAM']['oreb'],
                    game_dict[key]['home_box_score']['TEAM']['dreb'],
                    game_dict[key]['home_box_score']['TEAM']['ast'],
                    game_dict[key]['home_box_score']['TEAM']['stl'],
                    game_dict[key]['home_box_score']['TEAM']['blk'],
                    game_dict[key]['home_box_score']['TEAM']['to'],
                    game_dict[key]['home_box_score']['TEAM']['pf'],
                    game_dict[key]['home_box_score']['TEAM']['pts'],
                    game_dict[key]['home_box_score']['TEAM']['fastBreakPts'],
                    game_dict[key]['home_box_score']['TEAM']['ptsInPaint'],
                    game_dict[key]['home_box_score']['TEAM']['technicalFouls'],
                    game_dict[key]['home_box_score']['TEAM']['flagrantFouls'],
                    game_dict[key]['away_box_score']['TEAM']['fga'],
                    game_dict[key]['away_box_score']['TEAM']['fgm'],
                    game_dict[key]['away_box_score']['TEAM']['3pta'],
                    game_dict[key]['away_box_score']['TEAM']['3ptm'],
                    game_dict[key]['away_box_score']['TEAM']['fta'],
                    game_dict[key]['away_box_score']['TEAM']['ftm'],
                    game_dict[key]['away_box_score']['TEAM']['oreb'],
                    game_dict[key]['away_box_score']['TEAM']['dreb'],
                    game_dict[key]['away_box_score']['TEAM']['ast'],
                    game_dict[key]['away_box_score']['TEAM']['stl'],
                    game_dict[key]['away_box_score']['TEAM']['blk'],
                    game_dict[key]['away_box_score']['TEAM']['to'],
                    game_dict[key]['away_box_score']['TEAM']['pf'],
                    game_dict[key]['away_box_score']['TEAM']['pts'],
                    game_dict[key]['away_box_score']['TEAM']['fastBreakPts'],
                    game_dict[key]['away_box_score']['TEAM']['ptsInPaint'],
                    game_dict[key]['away_box_score']['TEAM']['technicalFouls'],
                    game_dict[key]['away_box_score']['TEAM']['flagrantFouls'],
                    game_dict[key]['odds_details']['line']['favorite'],
                    game_dict[key]['odds_details']['line']['margin'],
                    game_dict[key]['odds_details']['over/under']]    
        cursor.execute(SQLCommand, values)
        connection.commit()
        print('Import for game ' + key + ' committed')

print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
connection.close()
