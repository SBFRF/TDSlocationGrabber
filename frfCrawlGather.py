import csv, pickle, urllib.request, warnings
import numpy as np
import xml.etree.ElementTree as ET
from netCDF4 import Dataset

server = 'http://134.164.129.55'
#server = 'https://chlthredds.erdc.dren.mil'

outputName = 'database'

def main():
    
    urlList = getUrls()
    
    database, errorbase = buildDatabase(urlList)

    database = sortDatabase(database)

    database = collectLatLon(database)

    saveBinary(database)

    saveCsv(database)
    
    showErrors(errorbase)
    
def getUrls():
    
    if server == 'http://134.164.129.55':
        urlMain = server + '/thredds/catalog/FRF/oceanography/waves/catalog.xml'
    elif server == 'https://chlthredds.erdc.dren.mil':
        urlMain = server + '/thredds/catalog/frf/oceanography/waves/catalog.xml'
    else:
        print('Unknown server')
        quit()
    
    tree = ET.parse(urllib.request.urlopen(urlMain))
    root = tree.getroot()
    
    urlList = [None] * 999999
    
    i = 0
    for child in root[-1]:
        
        if child.tag[-10:] != 'catalogRef':
            continue
        urlChild = urlMain[:-11] + child.attrib[child.keys()[0]]
        tree = ET.parse(urllib.request.urlopen(urlChild))
        root = tree.getroot()
        for gchild in root[-1]:
            if gchild.tag[-10:] != 'catalogRef':
                continue
            urlGchild = urlChild[:-11] + gchild.attrib[gchild.keys()[0]]
            tree = ET.parse(urllib.request.urlopen(urlGchild))
            root = tree.getroot()
            for ggchild in root[-1]:
                if ggchild.tag[-7:] != 'dataset':
                    continue
                urlList[i] = '{}/thredds/dodsC/{}'.format(
                    server, ggchild.attrib['urlPath'])
                print('Found {}'.format('_'.join(
                    (urlList[i].split('/')[-1].split('_')[2:]))))
                i += 1
    
    urlList[i:] = []
    
    return urlList
                
def buildDatabase(urlList):

    database = dict()

    headers = ['Date', 'Type', 'Sensor', 'Lat', 'Lon', 'Url']

    for header in headers:
        database[header] = [None] * len(urlList)
    
    errorbase = dict()
    
    headers = ['OpeningError', 'LatLonError']
    
    for header in headers:
        errorbase[header] = [None] * 999
    
    i = 0
    j = 0
    k = 0

    for url in urlList:
        print('Parsing {}'.format('_'.join(
                    (url.split('/')[-1].split('_')[2:]))))
        
        try:
            rootgrp = Dataset(url)
        except OSError:
            print('Opening error')
            print('Url = ' + url)
            errorbase['OpeningError'][j] = url
            j += 1
            continue
            
        varList = list(rootgrp.variables)
        if 'latitude' in varList:
            lat = rootgrp['latitude'][:]
            lon = rootgrp['longitude'][:]
        elif 'lat' in varList:
            lat = rootgrp['lat'][:]
            lon = rootgrp['lon'][:]
        elif 'lidarLatitude' in varList:
            lat = rootgrp['lidarLatitude'][:]
            lon = rootgrp['lidarLongitude'][:]
        else:
            print('Lat/lon error')
            errorbase['LatLonError'][k] = url
            k += 1
            continue
        
        if (type(lat) is np.ma.core.MaskedConstant or type(lon) is 
        np.ma.core.MaskedConstant):
            print('Lat/lon error')
            errorbase['LatLonError'][k] = url
            k += 1
            continue
        
        rootgrp.close()
        
        date = int(url.split('_')[-1][:-3])
        sensor = url.split('_')[2]
        
        database['Date'][i] = date
        database['Type'][i] = url.split('_')[1]
        database['Sensor'][i] = sensor
        database['Lat'][i] = round(float(lat), 4)
        database['Lon'][i] = round(float(lon), 4)
        database['Url'][i] = url
        i += 1
        
    for key in database:
        database[key][i:] = []
        
    errorbase['OpeningError'][j:] = []
    errorbase['LatLonError'][k:] = []

    return database, errorbase

def sortDatabase(database):

    print('Sorting')

    # get sorting indices - sort by Sensor, then by Date
    ind = np.lexsort((database['Date'], database['Sensor']))[::-1]
    
    # sort each column using sorting indices
    for header in database:
        database[header] = list(np.array(database[header])[ind])

    return database

def collectLatLon(database):

    print('Gathering lat/lon')
    
    dbNew = dict()

    headers = ['DateStart', 'DateEnd', 'Type', 'Sensor', 'Lat', 'Lon', 'Url']

    for header in headers:
        dbNew[header] = [None] * 999

    dbNew['DateEnd'][0] = database['Date'][0]
    j = 0
    
    if server == 'http://134.164.129.55':
        frfTag = 'FRF'
    else:
        frfTag = 'frf'

    for i in range(len(database['Url'])):
        if (i == len(database['Url']) - 1 or database['Lat'][i] != 
        database['Lat'][i + 1] or database['Lon'][i] 
        != database['Lon'][i + 1]):
            dbNew['DateStart'][j] = database['Date'][i]
            dbNew['Type'][j] = database['Type'][i]
            dbNew['Sensor'][j] = database['Sensor'][i]
            dbNew['Lat'][j] = database['Lat'][i]
            dbNew['Lon'][j] = database['Lon'][i]
            
            url = ('{}/thredds/catalog/{}/oceanography/waves/{}/catalog.xml'
                .format(server, frfTag, database['Sensor'][i]))
            tree = ET.parse(urllib.request.urlopen(url))
            root = tree.getroot()
            foundNcml = False
            for child in root[-1]:
                if child.tag[-7:] == 'dataset':
                    dbNew['Url'][j] = '{}/thredds/dodsC/{}'.format(server, 
                        child.attrib['urlPath'])
                    foundNcml = True
                    break
      
            if not foundNcml:
                dbNew['Url'][j] = database['Url'][i]
            
            if i != len(database['Url']) - 1:
                j += 1
                dbNew['DateEnd'][j] = database['Date'][i + 1]
            else:
                break
    
    for key in dbNew:
        dbNew[key][j + 1:] = []

    return dbNew

def saveBinary(database):

    print('Saving binary')

    with open(outputName + '.p', 'wb') as outfile:
        pickle.dump(database, outfile)

def saveCsv(database):

    print('Saving csv')

    with open(outputName + '.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=database.keys())
        writer.writeheader()
        for i in range(len(database['Url'])):
            row = dict()
            for header in database:
                row[header] = database[header][i]
            writer.writerow(row)

def showErrors(errorbase):
    print('The following could not be opened:')
    for url in errorbase['OpeningError']:
        print(url)
    
    print('Lat/lon could not be found in the following:')
    for url in errorbase['LatLonError']:
        print(url)

if __name__ == '__main__':
    main()
