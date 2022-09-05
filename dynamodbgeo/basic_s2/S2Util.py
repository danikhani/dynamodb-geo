from s2sphere import LatLng as S2LatLng
from s2sphere import LatLngRect as S2LatLngRect


class S2Util:
    def __init__(self,config):
        self.config = config

    def latLngRectFromQueryRectangleInput(self, minLatitude, minLongitude, maxLatitude, maxLongitude):
        latLngRect = None
        if minLatitude is not None and minLongitude is not None and maxLatitude is not None and maxLongitude is not None:
            minLatLng = S2LatLng.from_degrees(
                minLatitude, minLongitude)
            maxLatLng = S2LatLng.from_degrees(
                maxLatitude, maxLongitude)
            latLngRect = S2LatLngRect.from_point_pair(minLatLng, maxLatLng)
        return latLngRect

    def getBoundingLatLngRectFromQueryRadiusInput(self, centerPointLatitude, centerPointLongitude, radiusInMeter):
        centerLatLng = S2LatLng.from_degrees(centerPointLatitude,centerPointLongitude)
        latReferenceUnit = -1.0 if centerPointLatitude > 0.0 else 1.0
        latReferenceLatLng = S2LatLng.from_degrees(centerPointLatitude + latReferenceUnit,
                                                   centerPointLongitude)
        lngReferenceUnit = -1.0 if centerPointLongitude > 0.0 else 1.0
        lngReferenceLatLng = S2LatLng.from_degrees(centerPointLatitude, centerPointLongitude
                                                   + lngReferenceUnit)
        latForRadius = radiusInMeter / \
            (centerLatLng.get_distance(latReferenceLatLng).radians * self.config.EARTH_RADIUS_METERS)
        lngForRadius = radiusInMeter / \
            (centerLatLng.get_distance(lngReferenceLatLng).radians * self.config.EARTH_RADIUS_METERS)
        minLatLng = S2LatLng.from_degrees(centerPointLatitude - latForRadius,
                                          centerPointLongitude - lngForRadius)
        maxLatLng = S2LatLng.from_degrees(centerPointLatitude + latForRadius,
                                          centerPointLongitude + lngForRadius)
        return S2LatLngRect(minLatLng, maxLatLng)
