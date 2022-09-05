
"""
Purpose: A signle point of entry for all the table and dynamodb client configuration
"""

class GeoDataManagerConfiguration:

    def __init__(self, tableName: str):
        self.tableName = tableName
        self.MERGE_THRESHOLD = 2  # still not clear
        self.hashKeyLength = 5
        # table setting
        self.partition_key_attribute = "partitionkey"
        self.sort_key_attribute = "sortkey"
        self.geohash = "geoHash"
        self.geo_json_attribute = "geoJson" # json location in a row.

        
        self.lsi_game_name = "lsi_game_name"  # name of the LSI
        self.gamename = "gameName"
        
        #self.gsi1_index_name = "find_user_"
        #self.gsi_hashkey = 
        #self.gsi_sort =
        
        #self.geoJsonPointType = "Point"  # for now only point is supported
        self.EARTH_RADIUS_METERS = 6367000.0
