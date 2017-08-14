from nba_games import create_gameList
import pypyodbc
import datetime




if __name__ == "__main__":
    print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))

    connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
    cursor = connection.cursor()
    
    start_dates = ['20160229']

    
    for date in start_dates:
        gameList = create_gameList(date)

        for game in gameList:

            gameID = game[0]
            #gameDate = game[1][0]
            awayTeamAbbr = game[1][0]
            homeTeamAbbr = game[1][1]
            SQLCommand = ("UPDATE dbo.TeamGameStats SET gameID2 = '%s' "%gameID+
                          "WHERE (gameDate = '2016-02-29'" +
                          "AND homeTeamAbbr = '%s' "%homeTeamAbbr +
                          "AND awayTeamAbbr = '%s') "%awayTeamAbbr)
                #print(SQLCommand)
            cursor.execute(SQLCommand)
            connection.commit()
            print('Update for game ' + gameID + ' committed.')

    
    print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    connection.close()
