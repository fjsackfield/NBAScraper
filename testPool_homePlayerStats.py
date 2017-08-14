from nba_boxscore_scraper_v2 import get_playerGameStats
from nba_boxscore_scraper_v2 import get_rowStats
from nba_boxscore_scraper_v2 import get_homeTeam
from nba_boxscore_scraper_v2 import get_awayTeam
from nba_games import create_gameURLList
import multiprocessing
import time
import datetime
from functools import partial
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import sys

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

    try:
        r = requests.get(gameURL, timeout=10)
        #time.sleep(2)
        
        if (r.status_code == 200):
            print('Processing.. ' + gameURL)
            html = r.text
            soup = BeautifulSoup(html, 'html.parser')
            boxSoup = soup.find_all(text='Basic Box Score Stats')  
            statRoots = [box.parent.parent.parent.nextSibling.nextSibling for box in boxSoup]
            data_rows = []
            for i in range(2):
                root = statRoots[i]
                for tr in root.find_all('tr'):
                    if tr.attrs != {}:
                        continue
                    else:
                        data_rows.append((i, str(tr)))

            #data_rows.remove(data_rows[5])
    except Exception as ex:
        print(str(ex))

    finally:
        return data_rows
       


def get_playerStats(gameURL, tup_data_row):
    #str_data_row is a tuple: (teamIndex, data_row)
    data_row = BeautifulSoup(tup_data_row[1], 'html.parser')
    print(data_row)
    playerStats = {}
    playerID = data_row.find('th').attrs['data-append-csv']
    playerStats[playerID] = {}

    gameID = gameURL.split('/')[4].split('.')[0]

    if tup_data_row[0] == 0:
        playerStats[playerID]['teamID'] = get_awayTeam(gameID)
    else:
        playerStats[playerID]['teamID'] = get_homeTeam(gameID)

    #Now loop through each box score column for the current box score row
    for data_stat in data_row.find_all('td'): #Each 'td' tag represents a box score column
        '''            
        if data_stat.attrs['data-stat'] == 'mp': #Convert minutes played to a float
            minList = data_stat.text.split(':')
            mins = round(float(int(minList[0])+(int(minList[1])/60)), 2)
            playerStats['mp'] = mins
                        
        elif 'pct' in data_stat.attrs['data-stat']: #No need to grab the pct stats
            continue
                    
        else: #Store the data_stats from each box score column inside the playerID dict 
            playerStats[data_stat.attrs['data-stat']] = int(data_stat.text)
        '''
        playerStats[playerID][data_stat.attrs['data-stat']] = data_stat.text
    print('Player dict created for '+playerID+' in game '+
          gameID + ' at ' +str(datetime.datetime.time(datetime.datetime.now())))        
    return playerStats


'''
if __name__ == "__main__":
    print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    
    gameURL = 'http://www.basketball-reference.com/boxscores/201210300CLE.html'
    home_data_rows = get_home_data_rows(gameURL)
    pool = multiprocessing.Pool(4)
    func = partial(get_homePlayerStats, gameURL)
    results = pool.map(func, [str(home_data_rows)])
    pool.close()
    pool.join()

    print(results.get())

    print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
'''
if __name__ == "__main__":
    #print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    start = time.time()
    gameURL = 'http://www.basketball-reference.com/boxscores/201210300CLE.html'
    data_rows = get_data_rows(gameURL)
    pool = multiprocessing.Pool(8)
    func = partial(get_playerStats, gameURL)
    for x in pool.map(func, data_rows):
        print("{} (Time elapsed: {}s)".format(x, int(time.time() - start)))
    pool.terminate()
    pool.join()

    #print(time.time() - start_time)
    #print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
