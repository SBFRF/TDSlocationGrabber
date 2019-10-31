from frfCrawlQuery import query
from frfCrawlGather import main as gather

date = (201601, 201906)
type_ = 'waves'
sensor = 'waverider-17m'
lat = (36.197, 36.200)
lon = (-75.715, -75.710)

gather()
query(date, type_, sensor, lat, lon)