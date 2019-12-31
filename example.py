import datetime as DT
from frfTDSdataCrawler import  query
start = DT.datetime(2015,10,3)
end = DT.datetime(2015,11,6)

type = 'waves'
dataLocations = query(start, end, type=type)