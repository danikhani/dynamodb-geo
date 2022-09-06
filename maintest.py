from dynamodbgeo.basic_dynamodbmanager import DynamoDBManager
from dynamodbgeo.basic_model import Covering
from dynamodbgeo.basic_config import GeoDataManagerConfiguration
from dynamodbgeo.basic_s2 import *

import boto3
import dynamodbgeo
import uuid  # used in range key uniquness

from s2sphere import RegionCoverer as S2RegionCoverer

from s2sphere import LatLng as S2LatLng
from s2sphere import LatLngRect as S2LatLngRect

class Querytest():
    def __init__(self,config,dynamodb_client):
        self.config = config
        self.dynamodb_client = dynamodb_client


    def dispatchQueries(self, covering: 'Covering'):
            """
            Generating multiple query from the covering area and running query on the DynamoDB table
            """
            
            ranges = covering.getGeoHashRanges(self.config.hashKeyLength)
            results = []
            for range in ranges:
                hashKey = S2Manager().generateHashKey(range.rangeMin, self.config.hashKeyLength)
                queried_geohash = self.queryGeohash(hashKey, range)
                results.extend(queried_geohash)
            return results

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


    def queryGeohash(self, hashKey: int, range: int):
        """
        Given a hash key and a min to max GeoHashrange it query the GSI to select the appropriate items to return
        """
        params={}

        params['TableName']=self.config.tableName
        params['IndexName']=self.config.lsi_game_name
        
        #  provide a specific value for the partition key
        # As eyConditionExpressions must only contain one condition per key, customer passing KeyConditionExpression will be replaced automatically
        params['KeyConditionExpression']=str(self.config.partition_key_attribute) + ' = :hashKey and ' + str(self.config.geohash) +' between :geohashMin and :geohashMax'
        
        expression_dic = {
            ':hashKey': {'N': str(hashKey)}, 
            ':geohashMax': { 'N': str(range.rangeMax)},
            ':geohashMin': {'N': str(range.rangeMin)}
        }
        if 'ExpressionAttributeValues' in params.keys():
            params['ExpressionAttributeValues'].update(expression_dic)
        else:
            params['ExpressionAttributeValues']= expression_dic
            

        response = self.dynamodb_client.query(**params)
        data = response['Items']

        # paginate if there are more for one request.
        while 'LastEvaluatedKey' in response: 
            params['ExclusiveStartKey']=response['LastEvaluatedKey']
            response = self.dynamodb_client.query(**params)
            data.extend(response['Items'])
        return data



dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1')
config = dynamodbgeo.GeoDataManagerConfiguration('test_8')


querytest = Querytest(config,dynamodb_client)
querytest.queryRectangle
