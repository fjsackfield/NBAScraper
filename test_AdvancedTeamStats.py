'''
This module defines a function create_testView(teamAbbr, gameSeason) that creates a list of
cumulative/average offensive and defensive stats to be calculated for teamAbbr in gameSeason. 
It then compiles together a list of 'subSubQueries' and stores them individually
into a 'subSubQueryList'. Each subSubQuery joins TeamGameStats b with a copy of itself,
TeamGameStats a (held in the next outer subQuery), to calculate a running total or average from
all of the games in TeamGameStats b leading up to each game in TeamGameStats a.
The cumulative stats calculated in the subSubQueries are:
    avgOffRtg, avgDefRtg, runningFGMTotal, runningFG
'''



import nba_games
import nba_teams
import pypyodbc
import datetime
import nba_boxscore_scraper



'''
teams = nba_teams.create_nba_team_dict()
teamAbbrs = []
for team in teams:
    teamAbbrs.append(teams[team]['teamAbbr'])
'''

def create_view(teamAbbr, gameSeason):
    connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
    cursor = connection.cursor()
    
    rawStats = ['OffRtg','DefRtg','fgm',
                'fga','ftm','fta', '3ptm', 
                'ast','oreb','dreb',
                'blk','stl','to',
                'fastBreakPts','ptsInPaint', 'adjAvgPoss']

    runningTeamStats = ['runningOffRtg','runningDefRtg',
                         'runningFGMTotal','runningFGATotal',
                         'runningFTMTotal','runningFTATotal',
                         'running3PTMTotal',
                         'runningAstTotal','runningOREBTotal',
                         'runningDREBTotal','runningBlkTotal',
                         'runningStlTotal','runningTOVTotal',
                         'runningFBPtsTotal','runningPtsinPaintTotal', 'runningAdjAvgPoss']

    rawOppStats = ['oreb','dreb', '3pta', 'fga']
    runningOppTeamStats = ['runningOppOREBTotal','runningOppDREBTotal',
                           'runningOpp3PTATotal','runningOppFGATotal']

    zipList = zip(runningTeamStats, rawStats)
    zipList2 = zip(runningOppTeamStats, rawOppStats)

    subSubQueryList = []

    #Build out the runningTotals subSubQuery List
    for (runningStat, stat) in zipList:
        if runningStat == 'runningAdjAvgPoss':
            subSubQuery = ("(SELECT SUM(b.adjAvgPoss) "+
                           "FROM dbo.TeamGameStats b WHERE b.gameID < a.gameID "+
                           "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                           "AND b.gameSeason = '%s'"%gameSeason+") AS %s"%runningStat)
        else:
            subSubQuery = ("(SELECT SUM(CASE WHEN (b.homeTeamAbbr = '%s') "%teamAbbr+
                                        "THEN b.[homeTeam.%s]"%stat+" ELSE b.[awayTeam.%s]"%stat+" END) "+
                          "FROM dbo.TeamGameStats b "+
                          "WHERE b.gameID < a.gameID "+
                          "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                          "AND b.gameSeason = '%s'"%gameSeason+") AS %s"%runningStat)
        subSubQueryList.append(subSubQuery)

    for (runningOppStat, oppStat) in zipList2:
        subSubQuery = ("(SELECT SUM(CASE WHEN (b.homeTeamAbbr = '%s') "%teamAbbr+
                                    "THEN b.[awayTeam.%s]"%oppStat+" ELSE b.[homeTeam.%s]"%oppStat+" END) "+
                      "FROM dbo.TeamGameStats b "+
                      "WHERE b.gameID < a.gameID "+
                      "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                      "AND b.gameSeason = '%s'"%gameSeason+") AS %s"%runningOppStat)
        subSubQueryList.append(subSubQuery)

    #Append the last running Game total subSubQuery
    subSubQueryList.append("(SELECT CASE WHEN COUNT(gameID) = 0 THEN NULL ELSE COUNT(gameID) END FROM dbo.TeamGameStats b "+
                           "WHERE b.gameID < a.gameID "+
                           "AND (b.homeTeamAbbr = '%s'"%teamAbbr+" OR b.awayTeamAbbr = '%s'"%teamAbbr+") "+
                           "AND b.gameSeason = '%s'"%gameSeason+") AS runningGameTotal")
    

    #Now that the subSubQuery List of runningTotals is complete, build out the middle subQuery,
    #of which all the subSubQueries are selections
    subQuery = ("(SELECT a.gameID, a.gameDate, " + "(SELECT (CASE WHEN (a.homeTeamAbbr = '%s') "%teamAbbr+
                   "THEN a.homeTeamAbbr ELSE a.awayTeamAbbr END)) AS teamAbbr, ")


    for query in subSubQueryList:
        subQuery = subQuery+query
        if query == subSubQueryList[20]:
            subQuery = subQuery+" "
        else:
            subQuery = subQuery+", "

    subQuery = (subQuery+"FROM dbo.TeamGameStats a "+
                "WHERE (a.homeTeamAbbr = '%s'"%teamAbbr+" OR a.awayTeamAbbr = '%s'"%teamAbbr+") "+
                "AND a.gameSeason = '%s'"%gameSeason+")")
        

    query = ("(SELECT c.gameID, c.gameDate, c.teamAbbr, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningOffRtg AS FLOAT)/CAST(c.runningGameTotal AS FLOAT))) AS avgOffRtg, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningDefRtg AS FLOAT)/CAST(c.runningGameTotal AS FLOAT))) AS avgDefRtg, "+
             "CONVERT(DECIMAL(10,2),(c.runningAdjAvgPoss/c.runningGameTotal)) AS pace, "+
             "CONVERT(DECIMAL(10,2),((c.runningFGMTotal+(0.5*c.running3PTMTotal))/c.runningFGATotal)*100) AS EFGPct, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningFTMTotal AS FLOAT)/CAST(c.runningFTATotal AS FLOAT))*100) AS FTPct, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningFTATotal AS FLOAT)/CAST(c.runningFGATotal AS FLOAT))*100) AS FTARate, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningOREBTotal AS FLOAT)/CAST((c.runningOREBTotal+c.runningOppDREBTotal) AS FLOAT))*100) AS OREBPct, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningDREBTotal AS FLOAT)/CAST((c.runningDREBTotal+c.runningOppOREBTotal) AS FLOAT))*100) AS DREBPct, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningAstTotal AS FLOAT)/CAST(c.runningFGMTotal AS FLOAT)*100)) AS AstRate, "+
             "CONVERT(DECIMAL(10,2),(c.runningStlTotal/c.runningAdjAvgPoss)*100) AS StlRate, "+
             "CONVERT(DECIMAL(10,2),(CAST(c.runningBlkTotal AS FLOAT)/CAST((c.runningOppFGATotal-c.runningOpp3PTATotal) AS FLOAT))*100) AS BlkRate, "+
             "CONVERT(DECIMAL(10,2),(c.runningTOVTotal/(c.runningFGATotal+0.44*c.runningFTATotal+c.runningTOVTotal))*100) AS TOVRate "+
             "FROM "+ subQuery + " c)")


    topView = ("(SELECT d.gameID, d.gameDate, d.teamAbbr, d.avgOffRtg, d.avgDefRtg, (d.avgOffRtg-d.avgDefRtg) AS NetRtg, d.pace, d.EFGPct,"+
               "d.FTPct,d.FTARate,d.OREBPct,"+
               "d.DREBPct,d.AstRate,d.StlRate,d.BlkRate,d.TOVRate FROM "+ query + "d)")

    view = ("CREATE VIEW VW_AdvTeamStats_2014_15_%s "%teamAbbr+
                       "AS "+ topView)
    #print(view)
    cursor.execute(view)
    connection.commit()
    print("Advanced Stats View created for %s"%teamAbbr+" in season %s"%gameSeason)
    connection.close()
    
