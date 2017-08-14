
import nba_games
import pypyodbc


connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

SQLCommand = ('INSERT INTO dbo.TeamGameStats(gameID, homeTeam, awayTeam,' +
'[homeTeam.fga], [homeTeam.fgm],' +
'[homeTeam.3pta], [homeTeam.3ptm], [homeTeam.fta], [homeTeam.ftm],' +
'[homeTeam.oreb], [homeTeam.dreb], [homeTeam.ast], [homeTeam.stl],' +
'[homeTeam.blk], [homeTeam.to], [homeTeam.pf],[homeTeam.pts],' +
'[awayTeam.fga], [awayTeam.fgm], [awayTeam.3pta], [awayTeam.3ptm],' +
'[awayTeam.fta], [awayTeam.ftm], [awayTeam.oreb], [awayTeam.dreb],' +
'[awayTeam.ast], [awayTeam.stl], [awayTeam.blk], [awayTeam.to], [awayTeam.pf], [awayTeam.pts],' +
'favorite, line, [over/under], [homeTeamWin?],' +
'[favoriteTeamWin?]) VALUES' +
'(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')

dateList = ['20170122']
for date in dateList:
    gameIDList = nba_games.create_IDList(date)
    game_dict = nba_games.create_nba_game_dict(gameIDList)
    
    for key in game_dict:
        if game_dict[key]['home_box_score']['TEAM']['pts'] > game_dict[key]['away_box_score']['TEAM']['pts']:
                homeTeamwon = 'Yes'
        else:
                homeTeamwon = 'No'
        values = [key, game_dict[key]['home_team'],
                    game_dict[key]['away_team'],
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
                    game_dict[key]['odds_details']['line']['favorite'],
                    game_dict[key]['odds_details']['line']['margin'],
                    game_dict[key]['odds_details']['over/under'],
                    homeTeamwon,
                    'NULL']    
        cursor.execute(SQLCommand, values)
        connection.commit()
        print('Import for game ' + key + ' committed')
    
connection.close()
