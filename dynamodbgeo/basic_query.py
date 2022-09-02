from basic_dynamodbmanager import DynamoDBManager
from basic_model import Covering
from s2sphere import RegionCoverer as S2RegionCoverer

EARTH_RADIUS_METERS = 6367000.0

from basic_s2 import *


from s2sphere import LatLng as S2LatLng
from s2sphere import LatLngRect as S2LatLngRect
EARTH_RADIUS_METERS = 6367000.0

geoJsonAttributeName = 
hashKeyLength = 
dynamoDBManager = DynamoDBManager(config)


def dispatchQueries(covering: 'Covering'):
    """
    Generating multiple query from the covering area and running query on the DynamoDB table
    """
    ranges = covering.getGeoHashRanges()
    results = []
    for range in ranges:
        hashKey = S2Manager().generateHashKey(range.rangeMin, hashKeyLength)
        queried_geohash = dynamoDBManager.queryGeohash(hashKey, range)
        results.extend(queried_geohash)
    return results


######
# Rectangle
######

def filterByRectangle(ItemList:'points retrieved from dynamoDB' , minLatitude, minLongitude, maxLatitude, maxLongitude):
        latLngRect = S2Util().latLngRectFromQueryRectangleInput(
            minLatitude, minLongitude, maxLatitude, maxLongitude)
        result = []
        for item in ItemList:
            geoJson = item[geoJsonAttributeName]["S"]
            coordinates = geoJson.split(",")
            latitude = float(coordinates[0])
            longitude = float(coordinates[1])
            latLng = S2LatLng.from_degrees(latitude, longitude)
            if(latLngRect.contains(latLng)):
                result.append(item)
        return result


def queryRectangle(minLatitude, minLongitude, maxLatitude, maxLongitude):
    latLngRect = S2Util().latLngRectFromQueryRectangleInput(
        minLatitude, minLongitude, maxLatitude, maxLongitude)

    s2RegionCoverer = S2RegionCoverer()
    coverings = s2RegionCoverer.get_covering(latLngRect)
    covering = Covering(coverings)
    results = dispatchQueries(covering)
    return filterByRectangle(results, minLatitude, minLongitude, maxLatitude, maxLongitude)


######
# Circle
######

def queryRadius(centerPointLatitude, centerPointLongitude, radiusInMeter, sort = True):
    latLngRect = S2Util().getBoundingLatLngRectFromQueryRadiusInput(centerPointLatitude, centerPointLongitude, radiusInMeter)
    s2RegionCoverer = S2RegionCoverer()
    coverings = s2RegionCoverer.get_covering(latLngRect)
    covering = Covering(coverings)
    
    results = dispatchQueries(covering)
    filtered_results = filterByRadius(results)
    if sort == True:
        # Tuples list (distance to the center point, the point data returned from dynamoDB)
        tuples = []
        centerLatLng = S2LatLng.from_degrees(centerPointLatitude, centerPointLongitude)
        for item in filtered_results:
            geoJson = item[geoJsonAttributeName]["S"]
            coordinates = geoJson.split(",")
            latitude = float(coordinates[0])
            longitude = float(coordinates[1])
            latLng = S2LatLng.from_degrees(latitude, longitude)
            tuples.append((centerLatLng.get_distance(
                latLng).radians * EARTH_RADIUS_METERS, item))
        tuples.sort(key=lambda x: x[0])  # Sort the list by distance (x [0] is the distance)
        return [item[1] for item in tuples]
    else:
        return filtered_results

def filterByRadius(ItemList: 'points retrieved from dynamoDB' ,centerPointLatitude, centerPointLongitude, radiusInMeter):
    centerPointLatitude, centerPointLongitude, radiusInMeter
    centerLatLng = S2LatLng.from_degrees(centerPointLatitude, centerPointLongitude)
    result = []
    for item in ItemList:
        geoJson = item[geoJsonAttributeName]["S"]
        coordinates = geoJson.split(",")
        latitude = float(coordinates[0])
        longitude = float(coordinates[1])
        latLng = S2LatLng.from_degrees(latitude, longitude)
        if(centerLatLng.get_distance(latLng).radians * EARTH_RADIUS_METERS < radiusInMeter):
            result.append(item)
    return result