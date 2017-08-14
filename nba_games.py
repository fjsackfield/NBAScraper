"""
nba_boxscore_sqlite.py
Author: Fred Sackfield
Date Created: 1-29-2017
Description: Module that imports espn_boxscore_scraper.py and loops through
             espn.com boxscore urls and stores the resulting dictionary
             of key[gameID]-value[boxscore data] pairs in a sqlite
             database
"""

from nba_boxscore_scraper import get_home_box_score
from nba_boxscore_scraper import get_away_box_score
from nba_boxscore_scraper import get_odds_details
from nba_boxscore_scraper import get_home_team
from nba_boxscore_scraper import get_away_team
from nba_boxscore_scraper import get_home_team_stats
from nba_boxscore_scraper import get_away_team_stats
from nba_boxscore_scraper import get_game_date
from nba_boxscore_scraper_v2 import get_playerGameStats

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
import urllib
import urllib.request as request
import requests
import datetime
import time
from time import sleep

#This function was used as a helper for create_gameList
def create_teamTups(start_date):
    url = 'http://www.espn.com/nba/schedule/_/date/' + start_date
    page = request.urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    teamTups = []
    
    days = soup.find_all('h2', limit=1)

    for day in days:
        print(day.text)
        #date = datetime.datetime.strptime(day.text, '%A, %B %d')
        year = start_date[:4]
        #gameDate = date.strftime('%m-%d')
        #gameDate = year+'-'+gameDate
        dayRoot = day.nextSibling
        abbrs = dayRoot.find_all('abbr')
        for i in range(0,int(len(abbrs)-1), 2):
            teamTups.append((abbrs[i].text, abbrs[i+1].text))

    return teamTups

#This function was used to help remediate gameIDs from ESPN source
#when gameIDs were initially from basketball-reference
def create_gameList(start_date):

    teamTups = create_teamTups(start_date)
    gameIDs = create_gameIDList(start_date)
    
    if len(gameIDs) != len(teamTups):
        print('Game count does not equal number of team tups.')
        return -1
    else:
        gameList = zip(gameIDs, teamTups)
        return gameList

#Creates and returns a list of all gameIDs for an NBA regular season week
#starting with start_date
def create_gameIDList(start_date):
    url = 'http://www.espn.com/nba/schedule/_/date/' + start_date
    page = request.urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    IDList = []

    game_count = len(soup.find_all('a', {'name': '&lpos=nba:schedule:score'}))
    atag = soup.find('a', {'name': '&lpos=nba:schedule:score'})
        
    for game in range(game_count):
        
        if atag.text == 'Postponed':
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})
        elif atag.text == 'Canceled':
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})
        elif 'WEST' in atag.text:
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})
        else:
            gameID = atag.attrs['href'].split('=')[1]
            IDList.append(gameID)
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})

    return IDList

#Does the same thing as create_gameIDList except it returns a list
#of boxscore URLs instead of gameIDs
def create_boxURLList(start_date):
    url = 'http://www.espn.com/nba/schedule/_/date/' + start_date
    page = request.urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    urlList = []

    game_count = len(soup.find_all('a', {'name': '&lpos=nba:schedule:score'}))
    atag = soup.find('a', {'name': '&lpos=nba:schedule:score'})
        
    for game in range(game_count):
        if atag.text == 'Postponed':
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})
        elif atag.text == 'Canceled':
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})
        elif 'WEST' in atag.text:
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})
        else:
            gameID = atag.attrs['href'].split('=')[1]
            urlList.append('http://www.espn.com/nba/boxscore?gameId='+gameID)
            atag = atag.find_next('a', {'name': '&lpos=nba:schedule:score'})

    return urlList  
            
def create_gameURLList(month, year):
    baseURL = 'http://www.basketball-reference.com/boxscores/'
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}

    pageURL = ('http://www.basketball-reference.com/leagues/NBA_%s'%year+
          '_games-%s.html'%month)

    try:
        r = requests.get(pageURL, headers=headers, timeout=10)
        if (r.status_code == 200):
            html = r.text
            soup = BeautifulSoup(html, 'html.parser')
            boxScores = soup.find_all('td', {'data-stat': 'box_score_text'})

            gameURLList = [baseURL+score.find('a').attrs['href'].split('/')[2].split('.')[0]+'.html'
                           for score in boxScores]
    except Exception as ex:
        print(str(ex))
    finally:
        return gameURLList


def create_player_game_dict(gameURLList):
    print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    player_game_dict = {}
    '''
    ##Code for concurrent url requests
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        #Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(get_playerGameStats, url): url for url in gameURLList}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
            else:
                gameID = url.split('/')[4].split('.')[0]
                player_game_dict[gameID] = {}
                player_game_dict[gameID]['gameDate'] = str_helper(gameID)
                player_game_dict[gameID]['playerStats'] = data
    
    print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))    
    return player_game_dict
    '''

def create_nba_game_dict(gameIDList):
    nba_game_dict = {}
    
    for ID in gameIDList:
        nba_game_dict[ID] = {}
        nba_game_dict[ID]['game_date'] = get_game_date(ID)
        nba_game_dict[ID]['home_team'] = get_home_team(ID)
        nba_game_dict[ID]['away_team'] = get_away_team(ID)
        nba_game_dict[ID]['home_box_score'] = get_home_box_score(ID)
        nba_game_dict[ID]['away_box_score'] = get_away_box_score(ID)
        nba_game_dict[ID]['odds_details'] = get_odds_details(ID)
        time.sleep(3)

    return nba_game_dict

def create_team_stat_dict(gameIDList):
    team_stat_dict = {}

    for ID in gameIDList:
        team_stat_dict[ID] = {}
        team_stat_dict[ID]['game_date'] = get_game_date(ID)
        team_stat_dict[ID]['home_team'] = get_home_team(ID)
        team_stat_dict[ID]['away_team'] = get_away_team(ID)
        team_stat_dict[ID]['home_team_stats'] = get_home_team_stats(ID)
        team_stat_dict[ID]['away_team_stats'] = get_away_team_stats(ID)
        team_stat_dict[ID]['odds_details'] = get_odds_details(ID)

    return team_stat_dict


def str_helper(gameID):
    tempList = []
    for x in gameID:
        tempList.append(x)

    datestr = ''
    for i in range(len(tempList)-4):
        if i == 4:
            datestr = datestr + '-' + tempList[i]
        elif i == 6:
            datestr = datestr + '-' + tempList[i]
        else:
            datestr += tempList[i]

    return datestr


    

