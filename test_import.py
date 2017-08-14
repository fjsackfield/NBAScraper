import nba_games
import nba_teams
import pypyodbc
import datetime



connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='TestNBAStats', trusted_connection='yes')
cursor = connection.cursor()

SQLCommand = ('INSERT INTO dbo.TestTeamGameStats(gameID, gameDate, homeTeamName, homeTeamAbbr,' +
'awayTeamName, awayTeamAbbr,' +
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
'(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')


#2015-16 Regular Season Import Complete on 2/10/17
#2014-15 Regular Season Import Complete on 2/14/17
#2013-14 Regular Season Import Complete on 2/19/17


game_dict = nba_games.create_team_stat_dict(['400277874', '400277875', '400277876'])
    
for key in game_dict:
    if game_dict[key]['home_team']['abbrev'] == game_dict[key]['away_team']['abbrev']:
        print(key)
        print(game_dict[key])
        break
    else:
        game_date = str(game_dict[key]['game_date'])
        values = [key, game_date, game_dict[key]['home_team']['name'],
                        game_dict[key]['home_team']['abbrev'],
                        game_dict[key]['away_team']['name'],
                        game_dict[key]['away_team']['abbrev'],
                        game_dict[key]['home_team_stats']['fga'],
                        game_dict[key]['home_team_stats']['fgm'],
                        game_dict[key]['home_team_stats']['3pta'],
                        game_dict[key]['home_team_stats']['3ptm'],
                        game_dict[key]['home_team_stats']['fta'],
                        game_dict[key]['home_team_stats']['ftm'],
                        game_dict[key]['home_team_stats']['oreb'],
                        game_dict[key]['home_team_stats']['dreb'],
                        game_dict[key]['home_team_stats']['ast'],
                        game_dict[key]['home_team_stats']['stl'],
                        game_dict[key]['home_team_stats']['blk'],
                        game_dict[key]['home_team_stats']['to'],
                        game_dict[key]['home_team_stats']['pf'],
                        game_dict[key]['home_team_stats']['pts'],
                        game_dict[key]['home_team_stats']['fastBreakPts'],
                        game_dict[key]['home_team_stats']['ptsInPaint'],
                        game_dict[key]['home_team_stats']['technicalFouls'],
                        game_dict[key]['home_team_stats']['flagrantFouls'],
                        game_dict[key]['away_team_stats']['fga'],
                        game_dict[key]['away_team_stats']['fgm'],
                        game_dict[key]['away_team_stats']['3pta'],
                        game_dict[key]['away_team_stats']['3ptm'],
                        game_dict[key]['away_team_stats']['fta'],
                        game_dict[key]['away_team_stats']['ftm'],
                        game_dict[key]['away_team_stats']['oreb'],
                        game_dict[key]['away_team_stats']['dreb'],
                        game_dict[key]['away_team_stats']['ast'],
                        game_dict[key]['away_team_stats']['stl'],
                        game_dict[key]['away_team_stats']['blk'],
                        game_dict[key]['away_team_stats']['to'],
                        game_dict[key]['away_team_stats']['pf'],
                        game_dict[key]['away_team_stats']['pts'],
                        game_dict[key]['away_team_stats']['fastBreakPts'],
                        game_dict[key]['away_team_stats']['ptsInPaint'],
                        game_dict[key]['away_team_stats']['technicalFouls'],
                        game_dict[key]['away_team_stats']['flagrantFouls'],
                        game_dict[key]['odds_details']['line']['favorite'],
                        game_dict[key]['odds_details']['line']['margin'],
                        game_dict[key]['odds_details']['over/under']]    
        cursor.execute(SQLCommand, values)
        connection.commit()
        print('Import for game ' + key + ' committed')

print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
connection.close()

