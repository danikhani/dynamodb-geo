#!/usr/bin/env python

"""
Purpose: Create the DynamoDB table for geo data based on the GeoDataManagerConfiguration. 

NOTE: for now the capacity is set to 10 RCU and 5 WCU to avoid high cost of batch writing.

TODO: Make the table configuration parametric.

01-create-table.py - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.client.run-application-python.01-create-table.html


Author: Hamza Rhibi
"""

class GeoTableUtil:

    def __init__(self,config,dynamoDBClient):
        self.config=config
        self.dynamoDBClient = dynamoDBClient
    
    def getCreateTableRequest(self):
        """
        Prepare dict with default parameter to create the base
        """
        params = {
            'TableName' : self.config.tableName,
            'KeySchema': [       
                { 'AttributeName': self.config.partition_key_attribute, 'KeyType': "HASH"},    # Partition key
                { 'AttributeName': self.config.sort_key_attribute, 'KeyType': "RANGE" }   # Sort key
            ],
            'AttributeDefinitions':[
                { 'AttributeName': self.config.partition_key_attribute, 'AttributeType': 'N' },
                { 'AttributeName': self.config.sort_key_attribute, 'AttributeType': 'S' },
                { 'AttributeName': self.config.geohash, 'AttributeType': 'N' },
                { 'AttributeName': self.config.gamename, 'AttributeType': 'S' }
                

            ],
            'LocalSecondaryIndexes':[
                {
                'IndexName': self.config.lsi_geohash_name,
                'KeySchema': [
                    {
                    'KeyType': 'HASH',
                    'AttributeName': self.config.partition_key_attribute
                    },
                    {
                    'KeyType': 'RANGE',
                    'AttributeName': self.config.geohash
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
                }, 
                {
                'IndexName': self.config.lsi_game_name,
                'KeySchema': [
                    {
                    'KeyType': 'HASH',
                    'AttributeName': self.config.partition_key_attribute
                    },
                    {
                    'KeyType': 'RANGE',
                    'AttributeName': self.config.gamename
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
                }
            ],
            'ProvisionedThroughput': {       
                'ReadCapacityUnits': 10, 
                'WriteCapacityUnits': 10
            }
        }
        return params


    def create_table(self,CreateTableInput :"Dict with parameters to create the table"):
        # skip if table already exists
        try:
            response = self.dynamoDBClient.describe_table(TableName=self.config.tableName)
            # table exists...bail
            print ("Table [{}] already exists. Skipping table creation.".format(self.config.tableName))
            return
        except:
            pass # no table... good
        print ("Creating table [{}]".format(self.config.tableName))

        table = self.dynamoDBClient.create_table(**CreateTableInput)

        print ("Waiting for table [{}] to be created".format(self.config.tableName))
        self.dynamoDBClient.get_waiter('table_exists').wait(TableName=self.config.tableName)
        # if no exception, continue
        print ("Table created")
        return

      