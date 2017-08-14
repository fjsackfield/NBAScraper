import nba_teams
import pypyodbc


connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

SQLCommand = ('INSERT INTO dbo.NBATeam(teamID, teamName, teamAbbr, ' +
              'teamDiv, teamConf) VALUES (?,?,?,?,?)')


team_dict = nba_teams.create_nba_team_dict()


for key in team_dict['EAST']:
    values = [team_dict['EAST'][key]['teamID'], key,
              team_dict['EAST'][key]['teamAbbr'],
              team_dict['EAST'][key]['teamDiv'], 'EAST']
    
    cursor.execute(SQLCommand, values)
    connection.commit()
    print('Import for team ' + key + ' committed')

for key in team_dict['WEST']:
    values = [team_dict['WEST'][key]['teamID'], key,
              team_dict['WEST'][key]['teamAbbr'],
              team_dict['WEST'][key]['teamDiv'], 'WEST']
    cursor.execute(SQLCommand, values)
    connection.commit()
    print('Import for team ' + key + ' committed')

connection.close()
    
