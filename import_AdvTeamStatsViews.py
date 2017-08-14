import nba_teams
from create_VW_AdvTeamStats import create_VW_AdvTeamStats

teams = nba_teams.create_nba_team_dict()
teamAbbrs = []

for team in teams:
    teamAbbrs.append(teams[team]['teamAbbr'])

gameSeason = '2015-16'

for abbr in teamAbbrs:
    create_VW_AdvTeamStats(abbr, gameSeason)


