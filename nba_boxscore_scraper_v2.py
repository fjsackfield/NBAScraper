'''
NBA Boxscore Scraper - version 2. Now using basketball-reference.com as the
source. Functions will be defined and used to import PlayerGameStats.
'''

import urllib
import urllib.request as request
from bs4 import BeautifulSoup
import requests
import time
import datetime


def get_awayTeam(gameID):
    url = 'http://www.basketball-reference.com/boxscores/'+gameID+'.html'
    req = request.urlopen(url)
    soup = BeautifulSoup(req, 'html.parser')

    strongSoup = soup.find_all('strong')

    awayTeamID = strongSoup[1].find('a').attrs['href'].split('/')[2]

    return awayTeamID


def get_homeTeam(gameID):
    url = 'http://www.basketball-reference.com/boxscores/'+gameID+'.html'
    req = request.urlopen(url)
    soup = BeautifulSoup(req, 'html.parser')

    strongSoup = soup.find_all('strong')

    homeTeamID = strongSoup[2].find('a').attrs['href'].split('/')[2]

    return homeTeamID

def get_awayPlayerGameStats(gameID):
    url = 'http://www.basketball-reference.com/boxscores/'+gameID+'.html'
    req = request.urlopen(url)
    soup = BeautifulSoup(req, 'html.parser')
    
    awayBoxSoup = soup.find_all(text='Basic Box Score Stats')[0]
    awayStatRoot = awayBoxSoup.parent.parent.parent.nextSibling.nextSibling

    awayPlayerGameStats = {}

    for row in awayStatRoot.find_all('tr'):
        playerStats = row.find_all('td')
        if len(playerStats) == 0:
            continue
        else: #Build out the awayPlayerGameStats[playerID] dict for each player
            playerID = row.find('th').attrs['data-append-csv']
            playerName = row.find('th').text
            awayPlayerGameStats[playerID] = {}
            awayPlayerGameStats[playerID]['teamID'] = get_awayTeam(gameID)
            awayPlayerGameStats[playerID]['playerName'] = playerName
            for td in playerStats:
                if td.attrs['data-stat'] == 'mp':
                    minList = td.text.split(':')
                    mins = round(float(int(minList[0])+(int(minList[1])/60)), 2)
                    awayPlayerGameStats[playerID]['mp'] = mins
                elif td.attrs['data-stat'] == 'plus_minus':
                    awayPlayerGameStats[playerID]['plusminus'] = td.text
                elif 'pct' in td.attrs['data-stat']:
                    continue
                else:
                    awayPlayerGameStats[playerID][td.attrs['data-stat']] = int(td.text)
    print('Away Player Stat Dict created for game '+ gameID)            
    return awayPlayerGameStats


def get_homePlayerGameStats(gameID):
    url = 'http://www.basketball-reference.com/boxscores/'+gameID+'.html'
    req = request.urlopen(url)
    soup = BeautifulSoup(req, 'html.parser')
    
    homeBoxSoup = soup.find_all(text='Basic Box Score Stats')[1]
    homeStatRoot = homeBoxSoup.parent.parent.parent.nextSibling.nextSibling

    homePlayerGameStats = {}

    for row in homeStatRoot.find_all('tr'):
        playerStats = row.find_all('td')
        if len(playerStats) == 0:
            continue
        else: #Build out the awayPlayerGameStats[playerID] dict for each player
            playerID = row.find('th').attrs['data-append-csv']
            playerName = row.find('th').text
            homePlayerGameStats[playerID] = {}
            homePlayerGameStats[playerID]['teamID'] = get_homeTeam(gameID)
            homePlayerGameStats[playerID]['playerName'] = playerName
            for td in playerStats:
                if td.attrs['data-stat'] == 'mp':
                    minList = td.text.split(':')
                    mins = round(float(int(minList[0])+(int(minList[1])/60)), 2)
                    homePlayerGameStats[playerID]['mp'] = mins
                elif td.attrs['data-stat'] == 'plus_minus':
                    homePlayerGameStats[playerID]['plusminus'] = td.text
                elif 'pct' in td.attrs['data-stat']:
                    continue
                else:
                    homePlayerGameStats[playerID][td.attrs['data-stat']] = int(td.text)
    print('Home Player Stat Dict created for game '+ gameID)               
    return homePlayerGameStats

'''
The two functions below, get_playerGameStats(gameURL) and
get_rowStats(gameURL, data_row, index) are used together to make
one playerGameStatsDict -- 20-22 outer keys: playerID ->
                                19 inner keys for each playerID: 1 playerName,
                                                                 1 teamID,
                                                                 & 17 boxscore stats

Need to work on improving processing speed by using multiprocessing Pool
Example from https://medium.com/python-pandemonium/how-to
-speed-up-your-python-web-scraper-by
-using-multiprocessing-f2f4ef838686#.6neio2a5j:

    p = Pool(10)
    playerGameDicts = p.map(get_playerGameStats, gameURLList)
    p.terminate()
    p.join

parse = get_playerGameStats
cars_links = create_gameURLList

My method takes ~5x as long as the example per URL.
In the example, a Pool with 10  speed increased by a factor of 
                               

'''
def get_playerGameStats(gameURL):
    print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    gameID = gameURL.split('/')[4].split('.')[0]
    playerGameStats = {}
    
    try:
        r = requests.get(gameURL, headers=headers, timeout=10)
        time.sleep(2)
        
        if (r.status_code == 200):
            print('Processing.. ' + gameURL)
            html = r.text
            soup = BeautifulSoup(html, 'html.parser')
            
            boxSoup = soup.find_all(text='Basic Box Score Stats')  
            i = 0
            
            for box in boxSoup:
                #From the 'Basic Box Score Stats' text, traverse back up the parser tree to find the data-rows
                statRoot = box.parent.parent.parent.nextSibling.nextSibling

                #Now loop through each box score row
                for data_row in statRoot.find_all('tr'):  #Each 'tr' tag represents a box score row
            
                    if len(data_row.find_all('td')) == 0: #If the box score row is the reserves/bench header row, then skip it
                        continue
            
                    else: #Build out the awayPlayerGameStats[playerID] dict for each player
                        #Get playerID from the 'th' tag to instantiate the playerID dict
                        playerID = data_row.find('th').attrs['data-append-csv']
                        playerGameStats[playerID] = get_rowStats(gameURL, data_row, i) #Call get_rowStats helper function to create each playerDict
            
                i = i+1
    except Exception as ex:
        print(str(ex))
        
    finally:
        print('Player Stat Dict created for game '+ gameID)
        print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
        return playerGameStats
    
'''
This function creates the inner dict "value" for each playerID "key".
The keys include playerName, teamID and 17 box score stats (19 total).
The resulting dict is then passed back to get_playerGameStats where it becomes the
value of the playerID key.
For each game, this function will be called for each player on both teams (~20-22 players),
and each time it is called, it loops through all 20 box score stats.
So in TOTAL, the get_playerGameStats loops through >400 iterations per game, which equates
to roughly 25-27 seconds per game.
'''

def get_rowStats(gameURL, data_row, index):
    playerDict = {}
    playerDict['playerName'] = data_row.find('th').text

    gameID = gameURL.split('/')[4].split('.')[0]
                
    if index == 0: #If it's the first pass of the boxSoup loop, then teamID = awayTeamID
        playerDict['teamID'] = get_awayTeam(gameID)
                    
    else: #If it's the second pass of the boxSoup loop, then teamID = homeTeamID
        playerDict['teamID'] = get_homeTeam(gameID)

    #Now loop through each box score column for the current box score row
    for data_stat in data_row.find_all('td'): #Each 'td' tag represents a box score column
                    
        if data_stat.attrs['data-stat'] == 'mp': #Convert minutes played to a float
            minList = data_stat.text.split(':')
            mins = round(float(int(minList[0])+(int(minList[1])/60)), 2)
            playerDict['mp'] = mins
                        
        elif 'pct' in data_stat.attrs['data-stat']: #No need to grab the pct stats
            continue
                    
        else: #Store the data_stats from each box score column inside the playerID dict 
            playerDict[data_stat.attrs['data-stat']] = int(data_stat.text)
            
    print('Player dict created for '+playerDict['playerName']+' in game '+
          gameID + ' at ' +str(datetime.datetime.time(datetime.datetime.now())))        
    return playerDict



