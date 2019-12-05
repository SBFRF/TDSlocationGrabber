
import datetime as DT
from frfCrawlQuery import  query
start = DT.datetime(2015,10,3)
end = DT.datetime(2015,11,6)

type = 'waves'
#i don't know what the best way to do this looks like
# if you did x and y, you'd need origin and orientation
# a lat/lon bounding box might be easiest: eg
polygon = [[[-75.742046, 36.150204],
            [-75.723574, 36.152968],
            [-75.698444, 36.107430],
            [-75.720473, 36.099559]]]  # these values are in southern shores right now so ssouldn't return any

GaugeNames_FollowThoseUsedInGetDataFRF, locationsInLatLon, urlsForData = query(start, end, type=type, polygon)