import pypyodbc
import itertools
import nba_teams



connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

abbrs = nba_teams.get_abbrs()

"""
matchups = itertools.combinations(abbrs, 2)

matchupList = []
for matchup in matchups:
    matchupList.append(matchup)

matchupPredictorQuery = ("")

for matchup in matchupList:
    matchupPredictorQuery += ("SELECT c.[favoriteTeamCover?], c.gameID, c.gameDate, c.favorite, "+
        "(CASE WHEN c.favorite = c.homeTeamAbbr THEN c.awayTeamAbbr ELSE c.homeTeamAbbr END) "+
        "AS underdog, c.line, "+ 
        "ABS(a.NetRtg-b.NetRtg) AS NetRtgDiff, "+
        "ABS(a.EFGPct-b.EFGPct) AS EFGPctDiff, "+
        "ABS(a.FTPct-b.FTPct) AS FTPctDiff, "+
        "ABS(a.FTARate-b.FTARate) AS FTARateDiff, "+
        "ABS(a.OREBPct-b.OREBPct) AS OREBPctDiff, "+
        "ABS(a.DREBPct-b.DREBPct) AS DREBPctDiff, "+
        "ABS(a.AstRate-b.AstRate) AS AstRateDiff, "+
        "ABS(a.StlRate-b.StlRate) AS StlRateDiff, "+
        "ABS(a.BlkRate-b.BlkRate) AS BlkRateDiff, "+
        "ABS(a.TOVRate-b.TOVRate) AS TOVRateDiff "+
        "FROM dbo.Test_VW_AdvTeamStats_2014_15_%s"%matchup[0]+ " a "
        "JOIN dbo.Test_VW_AdvTeamStats_2014_15_%s"%matchup[1]+ " b ON (a.gameID =  b.gameID) "+
        "JOIN dbo.TestTeamGameStats c ON (b.gameID = c.gameID)")
    if matchup == matchupList[434]:
        break
    else:
        matchupPredictorQuery += " UNION "

createView = ("CREATE VIEW Test_VW_MatchupPredictors_2014_15 AS "+
              "("+matchupPredictorQuery+")")
"""

unionQuery = ("")
for abbr in abbrs:
    unionQuery += ("SELECT * FROM dbo.VW_AdvTeamStats_2014_15_%s"%abbr)
    if abbr == abbrs[29]:
        break
    else:
        unionQuery += " UNION "


createUnionView = ("CREATE VIEW VW_AdvTeamStatsUnion_2014_15 AS "+
                   "("+unionQuery+")")


cursor.execute(createUnionView)
connection.commit()
print("AdvancedTeamStats Union View created for 2014-15 Season")
connection.close()








    

