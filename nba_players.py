'''
Python module that scrapes all NBA players (from 2012-13 season and on)
from basketball-reference and imports data into NBAPlayer table.
'''

from bs4 import BeautifulSoup
import urllib
import urllib.request as request
import datetime
import pypyodbc

connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

SQLCommand = ('INSERT INTO dbo.NBAPlayer(playerID, playerName, position,'+
              'height, weight, college, dob, firstYear, lastYear) VALUES' +
              '(?,?,?,?,?,?,?,?,?)')

letterList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p',
              'q','r','s','t','u','v','w','y','z']

playerDict = {}

for letter in letterList:
    url = 'http://www.basketball-reference.com/players/%s/'%letter
    request = urllib.request.urlopen(url)
    soup = BeautifulSoup(request, 'html.parser')

    maxyears = soup.find_all('td', {'data-stat': 'year_max'})
    players = []
    for year in maxyears:
        if (int(year.text) >= 2013):
            players.append(year.parent)
        else:
            continue

    for player in players:
        playerID = player.find('th').attrs['data-append-csv']
        playerDict[playerID] = {}
        playerDict[playerID]['playerName'] = player.find('th').text
        playerDict[playerID]['firstYear'] = player.find_all('td')[0].text
        playerDict[playerID]['lastYear'] = player.find_all('td')[1].text
        playerDict[playerID]['position'] = player.find_all('td')[2].text
        playerDict[playerID]['height'] = player.find_all('td')[3].text
        playerDict[playerID]['weight'] = int(player.find_all('td')[4].text)
        playerDict[playerID]['college'] = player.find_all('td')[6].text
        playerDOB = player.find_all('td')[5].text
        date = datetime.datetime.strptime(playerDOB, '%B %d, %Y')
        date = date.strftime('%Y-%m-%d')
        playerDict[playerID]['dob'] = date
    print('Player dicts created for letter %s.'%letter)
        
	
for key in playerDict:
    values = [key, playerDict[key]['playerName'], playerDict[key]['position'],
              playerDict[key]['height'], playerDict[key]['weight'],
              playerDict[key]['college'], playerDict[key]['dob'],
              playerDict[key]['firstYear'], playerDict[key]['lastYear']]
    cursor.execute(SQLCommand, values)
    connection.commit()
    print('Import for NBA Players committed')

connection.close()
    
    
