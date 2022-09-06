import boto3
import dynamodbgeo
import uuid  # used in range key uniquness


def put_point_in_db():
    print(" Testing the put ITem inside the rectengle ")
    sort_key = str(uuid.uuid4())
    geoDataManager = dynamodbgeo.DynamoDBManager(config, dynamodb_client)

    # define a dict of the item to input
    other_info_dic = {
        'Item': {
            'Country': {'S': "Italy"},
            'Capital': {'S': "Tunis"},
            'year': {'S': '2020'}
        },
        # ... Anything else to pass through to `putItem`, eg ConditionExpression
        'ConditionExpression': "attribute_not_exists(hashKey)"
    }

    other_info_dic = {
        'Item': {
            'gameName': {'S': "kl"},
        },
        # ... Anything else to pass through to `putItem`, eg ConditionExpression
        'ConditionExpression': "attribute_not_exists(hashKey)"
    }

    res = geoDataManager.put_Point(36.879163, 10.243123, sort_key,other_info_dic)
    print(res)


def query_rectangle():
    geoDataManager = dynamodbgeo.DynamoDBManager(config, dynamodb_client)
    query_generator = dynamodbgeo.QueryGenerator(config, geoDataManager)
    res = query_generator.queryRectangle(36.878184, 10.242358, 36.879317, 10.243648)
    print(res)

def query_circle():
    geoDataManager = dynamodbgeo.DynamoDBManager(config, dynamodb_client)
    query_generator = dynamodbgeo.QueryGenerator(config, geoDataManager)
    res = query_generator.queryRadius(36.879163, 10.243120,95)
    print(res)

def create_table():
    table_util = dynamodbgeo.GeoTableUtil(config,dynamodb_client)
    create_table_input = table_util.getCreateTableRequest()

    # tweaking the base table parameters
    create_table_input["ProvisionedThroughput"]['ReadCapacityUnits'] = 5

    # pass the input to create_table method
    table_util.create_table(create_table_input)

if __name__ == "__main__":

    dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1')
    config = dynamodbgeo.GeoDataManagerConfiguration('test_8')
    #create_table()

    #res = put_point_in_db()
    #print(res)

    res = query_rectangle()
    print(res)
    

