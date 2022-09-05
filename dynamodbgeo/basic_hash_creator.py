from basic_s2 import *

def generate_hash(Latitude, Longitude, hashKeyLength):
    s2_manager = S2Manager()
    # exakt geohash
    geohash = s2_manager.generateGeohash(Latitude, Longitude)
    # shorten key for query
    hashKey = s2_manager.generateHashKey(geohash, hashKeyLength)

    return_dic = {
        "geohash": geohash,
        "shorten_key": hashKey,
        "coordinates": "{},{}".format(Latitude,Longitude)
    }

    return return_dic