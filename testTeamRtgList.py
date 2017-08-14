import nba_games
import nba_teams
import pypyodbc
import datetime


teams = nba_teams.create_nba_team_dict()
teamAbbrs = []
for team in teams:
    teamAbbrs.append(teams[team]['teamAbbr'])


connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='TestNBAStats', trusted_connection='yes')
cursor = connection.cursor()


for teamAbbr in teamAbbrs:
    
    query = ("SELECT teamAbbr, gameDate, OffRtg, DefRtg "+
             "FROM dbo.Test_VW_AdvTeamStats_2014_15_%s "%teamAbbr+
             "WHERE gameDate = (SELECT MAX(gameDate) FROM "+
             "dbo.Test_VW_AdvTeamStats_2014_15_%s)"%teamAbbr)
    cursor.execute(query)

    for row in cursor.fetchall():
        print(row)

