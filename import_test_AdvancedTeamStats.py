import nba_teams
import test_AdvancedTeamStats

teams = nba_teams.create_nba_team_dict()
teamAbbrs = []

for team in teams:
    teamAbbrs.append(teams[team]['teamAbbr'])

gameSeason = '2014-15'

for abbr in teamAbbrs:
    test_AdvancedTeamStats.create_testView(abbr, gameSeason)


