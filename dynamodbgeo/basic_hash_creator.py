'''import GeoDataManagerConfiguration
from DynamoDBManager import DynamoDBManager
from model import PutPointInput, Covering, GetPointInput
from s2 import *
from s2sphere import LatLng as S2LatLng'''
from basic_s2 import *
EARTH_RADIUS_METERS = 6367000.0


from s2sphere import LatLng as S2LatLng
from s2sphere import Cell as S2Cell



def generate_hash(Latitude, Longitude, hashKeyLength):
    s2_manager = S2Manager()
    # exakt geohash
    geohash = s2_manager.generateGeohash(Latitude, Longitude)
    # shorten key for query
    hashKey = s2_manager.generateHashKey(geohash, hashKeyLength)

    return_dic = {
        "geohash": geohash,
        "shoren_key": hashKey,
        "coordinates": "{},{}".format(Latitude,Longitude),
        "Longitude": Longitude
    }

    return return_dic