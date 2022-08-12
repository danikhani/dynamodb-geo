import boto3
import dynamodbgeo
import uuid  # used in range key uniquness

if __name__ == "__main__":

    config = dynamodbgeo.GeoDataManagerConfiguration('geo_test_8')
    geoDataManager = dynamodbgeo.PointGeneretor(config)

    print(" Testing the put ITem inside the rectengle ")
    rangekey = str(uuid.uuid4())
    a = geoDataManager.generate_point(
        # latitude then latitude longitude
        dynamodbgeo.GeoPoint(36.879163, 10.243120),
        # Use this to ensure uniqueness of the hash/range pairs.
        rangekey
        )
    print(a)


    print(geoDataManager.queryRadius(
        dynamodbgeo.GeoPoint(36.874444, 10.241059),
        95, sort=True)
        )

    # testing the query rectangle method
    print(geoDataManager.queryRectangle(
        dynamodbgeo.GeoPoint(36.878184, 10.242358))
    )
