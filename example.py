
import datetime as DT
from frfCrawlQuery import  query
start = DT.datetime(2015,10,3)
end = DT.datetime(2015,11,6)

type = 'waves'
names, locations, urls = query(start, end, type=type)