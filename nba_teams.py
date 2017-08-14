'''
nba_players.py
Module to parse nba team rosters and store them in dicts
'''

import urllib
import urllib.request as request
from bs4 import BeautifulSoup

url = 'http://www.espn.com/nba/standings/_/group/divison'
request = request.urlopen(url)
soup = BeautifulSoup(request, 'html.parser')

def create_nba_team_dict():
    IDList = ['0001','0002','0003','0004','0005','0006','0007','0008','0009']
    IDBase = '00'
    for i in range(21):
        teamid = str(i+10)
        IDList.append(IDBase+teamid)

    nba_team_dict = {}


    AtlanticIndList = [1,2,11,13,14]
    CentralIndList = [0,5,6,7,9]
    SoutheastIndList = [3,4,8,10,12]

    NorthwestIndList = [18,21,22,23,27]
    PacificIndList = [15,19,25,28,29]
    SouthwestIndList = [16,17,20,24,26]

    name_root = soup.find('tr', {'class': 'standings-row'})
    abbr_roots = soup.find_all('abbr')

    names = []
    abbrs = []

    for i in range(30):
        name = name_root.find('span',{'class': 'team-names'})
        abbr = name_root.find('abbr')
        names.append(name.text)
        abbrs.append(abbr.text)
        name_root = name_root.find_next('tr', {'class': 'standings-row'})
        
    for i in range(3):
        for j in range(5):
            nba_team_dict[names[j+(i*5)]] = {}
            nba_team_dict[names[j+(i*5)]]['teamID'] = IDList[j+(i*5)]
            nba_team_dict[names[j+(i*5)]]['teamAbbr'] = abbrs[j+(i*5)]
            if ((j+(i*5)) in AtlanticIndList):
                nba_team_dict[names[j+(i*5)]]['teamDiv'] = 'Atlantic'
            elif ((j+(i*5)) in CentralIndList):
                nba_team_dict[names[j+(i*5)]]['teamDiv'] = 'Central'
            else:
                nba_team_dict[names[j+(i*5)]]['teamDiv'] = 'Southeast'
            nba_team_dict[names[j+(i*5)]]['teamConf'] = 'EAST'

    for i in range(3):
        for j in range(5):
            nba_team_dict[names[j+(i*5)+15]] = {}
            nba_team_dict[names[j+(i*5)+15]]['teamID'] = IDList[j+(i*5)+15]
            nba_team_dict[names[j+(i*5)+15]]['teamAbbr'] = abbrs[j+(i*5)+15]
            if ((j+(i*5)+15) in NorthwestIndList):
                nba_team_dict[names[j+(i*5)+15]]['teamDiv'] = 'Northwest'
            elif ((j+(i*5)+15) in SouthwestIndList):
                nba_team_dict[names[j+(i*5)+15]]['teamDiv'] = 'Southwest'
            else:
                nba_team_dict[names[j+(i*5)+15]]['teamDiv'] = 'Pacific'
            nba_team_dict[names[j+(i*5)+15]]['teamConf'] = 'WEST'                                             
    
    return nba_team_dict


def get_abbrs():
    teams = create_nba_team_dict()
    abbrs = []
    for team in teams:
        abbrs.append(teams[team]['teamAbbr'])

    return abbrs
        


