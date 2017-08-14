from nba_boxscore_scraper_v2 import get_playerGameStats
from nba_boxscore_scraper_v2 import get_rowStats
from nba_boxscore_scraper_v2 import get_homeTeam
from nba_boxscore_scraper_v2 import get_awayTeam
from nba_games import create_boxURLList
import multiprocessing
import time
import datetime
from functools import partial
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import sys
import pypyodbc
import re

start_time = time.time()

'''
A partial function has a callback function as its first argument.
And then the following arguments preset arguments to the callback function.
That way, for example, if you need to preset a gameURL to a parser function
before a map function is called (which takes only func and iterable as args),
you can use a partial function to define the callback function with the preset gameURL.

Then map will iterate through the partial function passing a list of arguments
to the callback function.

By defining a partial function for get_homePlayerStats, partial(get_homePlayerStats, 'https:..')
then we can pass partial function to map with a list of data_rows for get_homePlayerStats to parse


f = get_homePlayerStats
iterable = get_home_data_rows(gameURL)


for statRoot in statRoots
    for row in statRoot.find_all('tr')
        rows.append(row)

'''



def get_data_rows(gameURL):

    headers = {'header': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36'}
    try:
        r = requests.get(gameURL, headers=headers, timeout=60)
        time.sleep(5)
        
        if (r.status_code == 200):
            print('Processing.. ' + gameURL)
            html = r.text
            soup = BeautifulSoup(html, 'html.parser')
            names = soup.find_all(href=re.compile("/player/")) 
            data_rows = [str(name.parent.parent) for name in names]
            data_rows.append(soup.title.text.split(' - ')[2]) #the last item in data_rows will always be
                                                              #unformatted gameDate
    except Exception as ex:
        print(str(ex))

    finally:
        return data_rows
       


def get_playerStats(gameURL, str_data_row):
    #convert str_data_row back to soup
    data_row = BeautifulSoup(str_data_row, 'html.parser')
    print(data_row)
    playerStats = {}
    playerID = data_row.find('a').attrs['href'].split('/')[7]
    playerName = data_row.find('a').attrs['href'].split('/')[8]
    playerStats['playerName'] = playerName

    gameID = gameURL.split('=')[1]

    #Now loop through each box score column for the current box score row
    for data_stat in data_row.find_all('td'): #Each 'td' tag represents a box score column

        playerStats[data_stat.attrs['class'][0]] = data_stat.text

    
    print('Player dict created for '+playerName+' in game '+
          gameID + ' at ' +str(datetime.datetime.time(datetime.datetime.now())))        
    return (playerID, playerStats)



if __name__ == "__main__":
    
    print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    

    connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
    cursor = connection.cursor()

    SQLCommand = ('INSERT INTO dbo.PlayerGameStats(playerID, playerName, gameID, playerGameID, gameDate,'+
              'min, fga, fgm, [3pta], [3ptm], fta, ftm, oreb, dreb, ast, stl, blk, [to], pf,'+
              'plusminus, pts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
    
    SQLCommandDNP = ('INSERT INTO dbo.PlayerGameStats(playerID, playerName, gameID, playerGameID, gameDate,'+
                   'DNPreason) VALUES (?,?,?,?,?,?)')
    
    gameURLs = create_boxURLList('20170302')
    #gameURLs2 = create_boxURLList('20170302')
    #gameURLs3 = create_boxURLList('20170215')
    #gameURLs4 = create_boxURLList('20170206')
    '''
    for url in gameURLs2:
        gameURLs.append(url)
    
    for url in gameURLs3:
        gameURLs.append(url)
    for url in gameURLs4:
        gameURLs.append(url)
    '''
    for url in gameURLs:
        print(url)
        urlStart = time.time()
        data_rows = get_data_rows(url)

        #Pull the unformatted date from the end of the data_rows list,
        #then remove it before passing to map
        date = data_rows[len(data_rows)-1]
        date = datetime.datetime.strptime(date, '%B %d, %Y')
        gameDate = date.strftime('%Y-%m-%d')
        data_rows.remove(data_rows[len(data_rows)-1])
        
        pool = multiprocessing.Pool(8)
        func = partial(get_playerStats, url)
        playerGameList = []
        for x in pool.map(func, data_rows):
            playerGameList.append(x)
            #print("{} (Time elapsed: {}s)".format(x[0], int(time.time() - start)))

        pool.terminate()
        pool.join()
        gameID = url.split('=')[1]
        for playerGame in playerGameList: #playerGame is a tuple: (playerID, statDict)
            
            if len(playerGame[1]) < 4:
                values = [playerGame[0],playerGame[1]['playerName'], gameID,
                playerGame[0]+gameID, gameDate, playerGame[1]['dnp']]
                #cursor.execute(SQLCommandDNP, values)
                #connection.commit()
                #print('Import for player ' + playerGame[1]['playerName'] + 'in game '+ gameID + 'committed.')
                
            elif '-' in playerGame[1]['min']:
                values = [playerGame[0],playerGame[1]['playerName'], gameID,
                          playerGame[0]+gameID, gameDate, 'N/A']
                #cursor.execute(SQLCommandDNP, values)
                #connection.commit()
                #print('Import for player ' + playerGame[1]['playerName'] + 'in game '+ gameID + 'committed.')
                    
            else:
                values = [playerGame[0], playerGame[1]['playerName'], gameID, playerGame[0]+gameID, gameDate,
                                int(playerGame[1]['min']),
                                int(playerGame[1]['fg'].split('-')[1]),
                                int(playerGame[1]['fg'].split('-')[0]),
                                int(playerGame[1]['3pt'].split('-')[1]),
                                int(playerGame[1]['3pt'].split('-')[0]),
                                int(playerGame[1]['ft'].split('-')[1]),
                                int(playerGame[1]['ft'].split('-')[0]),
                                int(playerGame[1]['oreb']),
                                int(playerGame[1]['dreb']),
                                int(playerGame[1]['ast']),
                                int(playerGame[1]['stl']),
                                int(playerGame[1]['blk']),
                                int(playerGame[1]['to']),
                                int(playerGame[1]['pf']),
                                int(playerGame[1]['plusminus']),
                                int(playerGame[1]['pts'])]
                #print(values)
                #cursor.execute(SQLCommand, values)
                #connection.commit()
                #print('Import for player ' + playerGame[1]['playerName'] + ' in game '+
                    #  gameID + ' committed.')
                
        print("(Time elapsed for "+url+": {}s)".format(int(time.time() - urlStart)))
        
            
    print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    connection.close()
