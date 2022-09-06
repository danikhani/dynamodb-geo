from basic_dynamodbmanager import DynamoDBManager
from basic_model import Covering
from basic_config import GeoDataManagerConfiguration
from basic_s2 import *


from s2sphere import RegionCoverer as S2RegionCoverer

from s2sphere import LatLng as S2LatLng
from s2sphere import LatLngRect as S2LatLngRect


class QueryGenerator:
    def __init__(self, config:GeoDataManagerConfiguration, dynamoDBManager:DynamoDBManager):
        self.config = config
        self.dynamoDBManager = dynamoDBManager

    def dispatchQueries(self, covering: 'Covering'):
        """
        Generating multiple query from the covering area and running query on the DynamoDB table
        """
        
        ranges = covering.getGeoHashRanges(self.config.hashKeyLength)
        results = []
        for range in ranges:
            hashKey = S2Manager().generateHashKey(range.rangeMin, self.config.hashKeyLength)
            queried_geohash = self.dynamoDBManager.queryGeohash(hashKey, range)
            results.extend(queried_geohash)
        return results


    ######
    # Rectangle
    ######

    def filterByRectangle(self, ItemList:'points retrieved from dynamoDB', minLatitude, minLongitude, maxLatitude, maxLongitude):
        s2_util = S2Util(self.config)
        latLngRect = s2_util.latLngRectFromQueryRectangleInput(
            minLatitude, minLongitude, maxLatitude, maxLongitude)
        result = []
        for item in ItemList:
            geoJson = item[self.config.geo_json_attribute]["S"]
            coordinates = geoJson.split(",")
            latitude = float(coordinates[0])
            longitude = float(coordinates[1])
            latLng = S2LatLng.from_degrees(latitude, longitude)
            if(latLngRect.contains(latLng)):
                result.append(item)
        return result


    def queryRectangle(self, minLatitude, minLongitude, maxLatitude, maxLongitude):
        s2_util = S2Util(self.config)
        latLngRect = s2_util.latLngRectFromQueryRectangleInput(
            minLatitude, minLongitude, maxLatitude, maxLongitude)

        s2RegionCoverer = S2RegionCoverer()
        coverings = s2RegionCoverer.get_covering(latLngRect)
        covering = Covering(coverings)
        results = self.dispatchQueries(covering)
        return self.filterByRectangle(results, minLatitude, minLongitude, maxLatitude, maxLongitude)


    ######
    # Circle
    ######

    def queryRadius(self, centerPointLatitude, centerPointLongitude, radiusInMeter, sort = True):
        s2_util = S2Util(self.config)
        latLngRect = s2_util.getBoundingLatLngRectFromQueryRadiusInput(centerPointLatitude, centerPointLongitude, radiusInMeter)
        s2RegionCoverer = S2RegionCoverer()
        coverings = s2RegionCoverer.get_covering(latLngRect)
        covering = Covering(coverings)
        
        results = self.dispatchQueries(covering)
        filtered_results = self.filterByRadius(results,centerPointLatitude, centerPointLongitude, radiusInMeter)
        if sort == True:
            # Tuples list (distance to the center point, the point data returned from dynamoDB)
            tuples = []
            centerLatLng = S2LatLng.from_degrees(centerPointLatitude, centerPointLongitude)
            for item in filtered_results:
                geoJson = item[self.config.geo_json_attribute]["S"]
                coordinates = geoJson.split(",")
                latitude = float(coordinates[0])
                longitude = float(coordinates[1])
                latLng = S2LatLng.from_degrees(latitude, longitude)
                tuples.append((centerLatLng.get_distance(
                    latLng).radians * self.config.EARTH_RADIUS_METERS, item))
            tuples.sort(key=lambda x: x[0])  # Sort the list by distance (x [0] is the distance)
            return [item[1] for item in tuples]
        else:
            return filtered_results

    def filterByRadius(self, ItemList: 'points retrieved from dynamoDB' ,centerPointLatitude, centerPointLongitude, radiusInMeter):
        centerPointLatitude, centerPointLongitude, radiusInMeter
        centerLatLng = S2LatLng.from_degrees(centerPointLatitude, centerPointLongitude)
        result = []
        for item in ItemList:
            geoJson = item[self.config.geo_json_attribute]["S"]
            coordinates = geoJson.split(",")
            latitude = float(coordinates[0])
            longitude = float(coordinates[1])
            latLng = S2LatLng.from_degrees(latitude, longitude)
            if(centerLatLng.get_distance(latLng).radians * self.config.EARTH_RADIUS_METERS < radiusInMeter):
                result.append(item)
        return result