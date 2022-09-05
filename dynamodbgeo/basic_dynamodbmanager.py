"""
Purpose: This class contains all the operation to perform on the DynamoDB table

"""
from basic_s2 import S2Manager
from boto3.dynamodb.conditions import Key, Attr


class DynamoDBManager:

    def __init__(self, config, dynamodb_client):
        self.config = config
        self.dynamodb_client = dynamodb_client 

    # for now we're not taking params passed into queryInput in consideration
    def queryGeohash(self, hashKey: int, range: int):
        """
        Given a hash key and a min to max GeoHashrange it query the GSI to select the appropriate items to return
        """
        params={}

        params['TableName']=self.config.tableName
        params['IndexName']=self.config.geohashIndexName
        
        # As eyConditionExpressions must only contain one condition per key, customer passing KeyConditionExpression will be replaced automatically
        params['KeyConditionExpression']=str(self.config.hashKeyAttributeName) + ' = :hashKey and ' + str(self.config.geohashAttributeName) +' between :geohashMin and :geohashMax'
        
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

        while 'LastEvaluatedKey' in response: 
            params['ExclusiveStartKey']=response['LastEvaluatedKey']
            response = self.dynamodb_client.query(**params)
            data.extend(response['Items'])
        return data


    def put_Point(self, Latitude, Longitude, sort_key, extra_params_dic):
        """
        The dict in Item put_item call, should contains a dict with string as a key and a string as a value: {"N": "123"}
        """

        s2_manager = S2Manager()
        geohash = s2_manager.generateGeohash(Latitude, Longitude)
        hashKey = s2_manager.generateHashKey(geohash, self.config.hashKeyLength)
        response = ""
        params = extra_params_dic.copy()   

        params['TableName']=self.config.tableName
        
        if('Item' not in extra_params_dic.keys()):
            params['Item']={}

        params['Item'][self.config.hashKeyAttributeName] ={"N": str(hashKey)}
        params['Item'][self.config.rangeKeyAttributeName] ={"S": sort_key}
        params['Item'][self.config.geohashAttributeName] ={'N': str(geohash)}
        params['Item'][self.config.geoJsonAttributeName] ={"S": "{},{}".format(Latitude, Longitude)}
        
        try:
            response = self.config.dynamoDBClient.put_item(**params)
        except Exception as e:
            print("The following error occured during the item insertion :{}".format(e))
            response = "Error"
        return response

    def get_Point(self, Latitude, Longitude, sort_key):
        """
        The dict in Key get_item call, should contains a dict with string as a key and a string as a value: {"N": "123"}
        """
        geohash = S2Manager().generateGeohash(Latitude, Longitude)
        hashKey = S2Manager().generateHashKey(geohash, self.config.hashKeyLength)
        response = ""
        try:
            response = self.config.dynamoDBClient.get_item(
                TableName=self.config.tableName,
                Key={
                    self.config.hashKeyAttributeName: {"N": str(hashKey)},
                    self.config.rangeKeyAttributeName: {
                        "S": sort_key}
                }
            )
        except Exception as e:
            print("The following error occured during the item retrieval :{}".format(e))
            response = "Error"
        return response
    
    def update_Point(self, Latitude, Longitude, sort_key, extra_params_dic):
        """
        The dict in Item Update call, should contains a dict with string as a key and a string as a value: {"N": "123"}
        """
        geohash = S2Manager().generateGeohash(Latitude, Longitude)
        hashKey = S2Manager().generateHashKey(geohash, self.config.hashKeyLength)
        response = ""
        params=extra_params_dic.copy()   

        params['TableName']=self.config.tableName
        
        if('Key' not in extra_params_dic.keys()):
            params['Key']={}

        params['Key'][self.config.hashKeyAttributeName] ={"N": str(hashKey)}
        params['Key'][self.config.rangeKeyAttributeName] ={"S": sort_key}
        
        #TODO Geohash and geoJson cannot be updated. For now no control over that need to be added        
        try:
            response = self.config.dynamoDBClient.update_item(**params)
        except Exception as e:
            print("The following error occured during the item update :{}".format(e))
            response = "Error"
        return response

    def delete_Point(self, Latitude, Longitude, sort_key, extra_params_dic):
        """
        The dict in Item Update call, should contains a dict with string as a key and a string as a value: {"N": "123"}
        """
        geohash = S2Manager().generateGeohash(Latitude, Longitude)
        hashKey = S2Manager().generateHashKey(geohash, self.config.hashKeyLength)
        response = ""
        params=extra_params_dic.copy()   

        params['TableName']=self.config.tableName
        
        if('Key' not in extra_params_dic.keys()):
            params['Key']={}

        params['Key'][self.config.hashKeyAttributeName] ={"N": str(hashKey)}
        params['Key'][self.config.rangeKeyAttributeName] ={"S": sort_key}
        try:
            response = self.config.dynamoDBClient.delete_item(**params)
        except Exception as e:
            print("The following error occured during the item delete :{}".format(e))
            response = "Error"
        return response

