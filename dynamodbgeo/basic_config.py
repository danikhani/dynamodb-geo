
"""
Purpose: A signle point of entry for all the table and dynamodb client configuration
"""

class GeoDataManagerConfiguration:

    MERGE_THRESHOLD = 2  # still not clear

    geohashIndexName = "geohash-index"  # name of the LSI

    def __init__(self, dynamoDBClient, tableName: str):
        self.dynamoDBClient = dynamoDBClient  # dynamodb client taken from aws sdk
        self.tableName = tableName
        self.hashKeyAttributeName = "hashKey"
        self.rangeKeyAttributeName = "rangeKey"
        self.geohashAttributeName = "geohash"
        self.geoJsonAttributeName = "geoJson"
        self.hashKeyLength = 2
        self.geoJsonPointType = "Point"  # for now only point is supported
        self.EARTH_RADIUS_METERS = 6367000.0
