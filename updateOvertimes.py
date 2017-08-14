import urllib
import urllib.request
from bs4 import BeautifulSoup
import pypyodbc
import datetime


connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

urlbase = 'http://www.basketball-reference.com/leagues/NBA_2017_games-'

year1 = '2016%'
year2 = '2017%'
months1 = ['october','november','december']
months2 = ['january','february']

for month in months1:
    url = urlbase + month + '.html'
    print(url)
    request = urllib.request.urlopen(url)
    soup = BeautifulSoup(request, 'html.parser')
    ots = soup.find_all('td', {'data-stat': 'overtimes'})

    for ot in ots:
        if ot.text == '':
            continue
        else:
            tempList = ot.text.split('O')
            if tempList[0] == '':
                otNum = 1
            else:
                otNum = tempList[0]
        awayTeamName = ot.parent.find('td', {'data-stat':'visitor_team_name'}).text
        homeTeamName = ot.parent.find('td', {'data-stat':'home_team_name'}).text
        awayTeamPts = int(ot.parent.find('td', {'data-stat':'visitor_pts'}).text)
        homeTeamPts = int(ot.parent.find('td', {'data-stat':'home_pts'}).text)
        SQLCommand = ("UPDATE dbo.TeamGameStats SET overtimes = %s "%otNum+
                      "WHERE gameDate LIKE '%s' "%year1+
                      "AND homeTeamName = '%s' "%homeTeamName+
                      "AND awayTeamName = '%s' "%awayTeamName+
                      "AND [homeTeam.pts] = %s "%homeTeamPts+
                      "AND [awayTeam.pts] = %s"%awayTeamPts)
        
        cursor.execute(SQLCommand)
        connection.commit()
        print('Update committed for %s'%homeTeamName+' vs %s'%awayTeamName+
              ' in %s'%month+ ' %s'%year1)
connection.close()



connection = pypyodbc.connect(driver='{SQL Server}',server='HP-FSACKFIELD\SQLEXPRESS', database='NBAStats', trusted_connection='yes')
cursor = connection.cursor()

for month in months2:
    url = urlbase + month + '.html'
    request = urllib.request.urlopen(url)
    soup = BeautifulSoup(request, 'html.parser')
    ots = soup.find_all('td', {'data-stat': 'overtimes'})

    for ot in ots:
        if ot.text == '':
            continue
        else:
            tempList = ot.text.split('O')
            if tempList[0] == '':
                otNum = 1
            else:
                otNum = tempList[0]
        awayTeamName = ot.parent.find('td', {'data-stat':'visitor_team_name'}).text
        homeTeamName = ot.parent.find('td', {'data-stat':'home_team_name'}).text
        awayTeamPts = int(ot.parent.find('td', {'data-stat':'visitor_pts'}).text)
        homeTeamPts = int(ot.parent.find('td', {'data-stat':'home_pts'}).text)
        SQLCommand = ("UPDATE dbo.TeamGameStats SET overtimes = %s "%otNum+
                      "WHERE gameDate LIKE '%s' "%year2+
                      "AND homeTeamName = '%s' "%homeTeamName+
                      "AND awayTeamName = '%s' "%awayTeamName+
                      "AND [homeTeam.pts] = %s "%homeTeamPts+
                      "AND [awayTeam.pts] = %s"%awayTeamPts)
        cursor.execute(SQLCommand)
        connection.commit()
        print('Update committed for %s'%homeTeamName+' vs %s'%awayTeamName+
              ' in %s'%month+ ' %s'%year2)


        
connection.close()
    


