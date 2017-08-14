import nba_games
import nba_teams
import pypyodbc
import datetime



print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))


connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='TestNBAStats', trusted_connection='yes')
cursor = connection.cursor()

gameSeasons = ['2014-15']
teams = nba_teams.create_nba_team_dict()
teamAbbrs = []
for team in teams:
    teamAbbrs.append(teams[team]['teamAbbr'])
print(teamAbbrs)

for gameSeason in gameSeasons:
    for teamAbbr in teamAbbrs:
        SQLCommand = ("CREATE VIEW TEST_VW_AdvancedTeamStats_2014_15_%s "%teamAbbr+
                      "AS (SELECT c.gameID, c.gameDate,"+
                      "CONVERT(DECIMAL(10,1),(c.runningPtsTotal/c.runningPossTotal)*100) AS OffRtg, "+
                      "CONVERT(DECIMAL(10,1),(c.runningOppPtsTotal/c.runningOppPossTotal)*100) AS DefRtg FROM "+
                            "(SELECT a.gameID, a.gameDate, a.homeTeamAbbr, a.[homeTeam.pts], a.[homeTeam.poss],"+
                                     "(SELECT SUM(b.[homeTeam.pts]) "+
                                      "FROM dbo.TestTeamGameStats b "+
                                      "WHERE b.gameID <= a.gameID "+
                                      "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                                      "AND b.gameSeason = '%s'"%gameSeason+") AS runningPtsTotal,"+
                                     "(SELECT SUM(b.[homeTeam.poss]) "+
                                      "FROM dbo.TestTeamGameStats b "+
                                      "WHERE b.gameID <= a.gameID "+
                                      "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                                      "AND b.gameSeason = '%s'"%gameSeason+") AS runningPossTotal,"+
                                     "(SELECT SUM(b.[awayTeam.pts]) "+
                                      "FROM dbo.TestTeamGameStats b "+
                                      "WHERE b.gameID <= a.gameID "+
                                      "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                                      "AND b.gameSeason = '%s'"%gameSeason+") AS runningOppPtsTotal,"+
                                     "(SELECT SUM(b.[awayTeam.poss]) "+
                                      "FROM dbo.TestTeamGameStats b "+
                                      "WHERE b.gameID <= a.gameID "+
                                      "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                                      "AND b.gameSeason = '%s'"%gameSeason+") AS runningOppPossTotal "+
                            "FROM dbo.TestTeamGameStats a "+
                            "WHERE (a.homeTeamAbbr = '%s'"%teamAbbr+" OR a.awayTeamAbbr = '%s'"%teamAbbr+") "+
                            "AND a.gameSeason = '%s'"%gameSeason+") c)")
        cursor.execute(SQLCommand)
        connection.commit()
        print('Test view created for team ' + teamAbbr + ' in season ' + gameSeason)
        
print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
connection.close()
