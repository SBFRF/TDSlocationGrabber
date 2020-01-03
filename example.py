import datetime as DT
from frfTDSdataCrawler import  query
start = DT.datetime(2015,10,3)
end = DT.datetime(2015,11,6)

type = 'waves'
# make sure that frfTDSdataCrawler has run and generated a database.p file
dataLocations = query(start, end,inputName='database', type='waves')
for tt in range(len(dataLocations['Sensor'])):
    print("gauge {} was at lon {}, lat {} from {} to {}".format(dataLocations['Sensor'][tt], dataLocations['Lon'][tt],
                                dataLocations['Lat'][tt], dataLocations['DateStart'][tt].date(), dataLocations['DateEnd'][tt].date()))
