import pypyodbc
import itertools
import nba_teams




def create_VW_AdvTeamStatsUnion(gameSeason):
    
    connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
    cursor = connection.cursor()

    unionQuery = ("")

    viewDateLst = gameSeason.split('-')
    viewDateStr = viewDateLst[0]+'_'+viewDateLst[1]

    abbrs = nba_teams.get_abbrs()
    
    for abbr in abbrs:
        unionQuery += ("SELECT * FROM dbo.VW_AdvTeamStats_%s"%viewDateStr+"_%s"%abbr)
        if abbr == abbrs[29]:
            break
        else:
            unionQuery += " UNION "


    createUnionView = ("CREATE VIEW VW_AdvTeamStatsUnion_%s"%viewDateStr+" AS "+
                       "("+unionQuery+")")


    cursor.execute(createUnionView)
    connection.commit()
    print("AdvancedTeamStats Union View created for "+gameSeason+" Season")
    connection.close()








    

