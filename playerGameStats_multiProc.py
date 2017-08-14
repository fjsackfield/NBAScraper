from nba_boxscore_scraper_v2 import get_playerGameStats
from nba_games import create_gameURLList
import multiprocessing
import time
import datetime


def map_urls():
    print('Start time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    player_game_dict = {}
    gameURLList = create_gameURLList('october', '2013')
    gameIDs = [url.split('/')[4].split('.')[0] for url in gameURLList]
    pool = multiprocessing.Pool(3)
    i = 0
    for playerGameDict in pool.map(get_playerGameStats, gameURLList):
        print('Mapping ' + gameIDs[i])
        player_game_dict[gameIDs[i]] = playerGameDict
        i += 1

    pool.terminate()
    pool.join()
    print('End time: ' + str(datetime.datetime.time(datetime.datetime.now())))
    return player_game_dict


if __name__ == "__main__":
    map_urls()
    
    
