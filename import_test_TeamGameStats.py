import nba_games
import nba_teams
import pypyodbc
import datetime


print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
teams = nba_teams.create_nba_team_dict()
print(teams)


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

SQLCommand_2 = ('INSERT INTO dbo.TeamGameStats(gameID, gameDate, homeTeamName, homeTeamAbbr, homeTeamID,' +
'awayTeamName, awayTeamAbbr, awayTeamID) VALUES' +
'(?,?,?,?,?,?,?,?)')

gameID = '400899552'
game_dict = nba_games.create_nba_game_dict([gameID])
for key in game_dict:
    date = str(game_dict[key]['game_date'])
    values = [key, date, game_dict[key]['home_team']['name'],
                    game_dict[key]['home_team']['abbrev'],
                    teams[game_dict[key]['home_team']['name']]['teamID'],
                    game_dict[key]['away_team']['name'],
                    game_dict[key]['away_team']['abbrev'],
                    teams[game_dict[key]['away_team']['name']]['teamID']]

    for value in values:
        if type(value) == str:
            print(value)
            print(len(value))
        else:
            continue
    cursor.execute(SQLCommand_2, values)
    connection.commit()
    print('Import for game ' + key + ' committed')

connection.close()
