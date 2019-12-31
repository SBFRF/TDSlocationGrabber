import csv, pickle, urllib.request, warnings, sys, time
import numpy as np
import xml.etree.ElementTree as ET
from netCDF4 import Dataset
import progressbar

def main(server):
    """main code structure"""

    datatype = ['waves', 'currents']

    urlList = getUrls(server, datatype)
    
    database, errorbase = buildDatabase(urlList)

    database = sortDatabase(database)

    database = collectLatLon(database)

    outputName = 'database'

    saveBinary(outputName, database)

    saveCsv(outputName, database)
    
    showErrors(errorbase)
    
def getUrls(server, datatype):

    urlList = [None] * 999999
    istart = 0
    
    for datatype_ in datatype:
        urlList, istart = getUrlsEachType(server, datatype_, urlList, istart)
    
    urlList[istart:] = []
    
    return urlList
    
def getUrlsEachType(server, datatype, urlList, istart):
    
    if server == 'http://134.164.129.55':
        urlMain = (server + '/thredds/catalog/FRF/oceanography/{}/catalog.xml'
            .format(datatype))
    elif server == 'https://chldata.erdc.dren.mil':
        urlMain = (server + '/thredds/catalog/frf/oceanography/{}/catalog.xml'
            .format(datatype))
    else:
        print('Unknown server')
        quit()
    
    tree = ET.parse(urllib.request.urlopen(urlMain))
    root = tree.getroot()
    
    i = istart
    bar = progressbar.ProgressBar(maxval=len(root[-1]),
                    widgets=[progressbar.Bar('.', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    for ii, child in enumerate(root[-1]):
        bar.update(ii+1)
        if child.tag[-10:] != 'catalogRef':
            continue
        urlChild = urlMain[:-11] + child.attrib[child.keys()[0]]
        tree = ET.parse(urllib.request.urlopen(urlChild))
        root = tree.getroot()
        for gchild in root[-1]:
            if gchild.tag[-10:] != 'catalogRef':
                continue
            urlGchild = urlChild[:-11] + gchild.attrib[gchild.keys()[0]]
            t, itMax = 0, 10
            while t < itMax:
                try:
                    tree = ET.parse(urllib.request.urlopen(urlGchild))
                    break
                except:
                    time.sleep(10)
                    t += 1
                    continue
            root = tree.getroot()
            for ggchild in root[-1]:
                if ggchild.tag[-7:] != 'dataset':
                    continue
                urlList[i] = '{}/thredds/dodsC/{}'.format(
                    server, ggchild.attrib['urlPath'])
                # print('Found {}'.format('_'.join(
                #     (urlList[i].split('/')[-1].split('_')[2:]))))
                i += 1
    bar.finish()
    return urlList, i
                
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
    bar = progressbar.ProgressBar(maxval=len(urlList),
                    widgets=[progressbar.Bar('.', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    print("Begin Parsing found files")
    for ii, url in enumerate(urlList):
        for attempt in range(10):
            try:
                # print('Parsing {}'.format('_'.join(
                #             (url.split('/')[-1].split('_')[2:]))))
                bar.update(ii+1)
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
            except:
                print('\n\r retrying {}'.format('_'.join((url.split('/')[-1].split('_')[2:]))))
                continue
            else:
                break
    bar.finish()
    for key in database:
        database[key][i:] = []
        
    errorbase['OpeningError'][j:] = []
    errorbase['LatLonError'][k:] = []

    return database, errorbase

def sortDatabase(database):

    print('Sorting')

    # get sorting indices - sort by Sensor, then by Date
    ind = np.lexsort((database['Date'], database['Sensor'], database['Type']
        ))[::-1]
    bar = progressbar.ProgressBar(maxval=len(database),
                    widgets=[progressbar.Bar('.', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    # sort each column using sorting indices
    for ii, header in enumerate(database):
        database[header] = list(np.array(database[header])[ind])
        bar.update(ii+1)
    bar.finish()
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

    bar = progressbar.ProgressBar(maxval=len(database['Url']),
                    widgets=[progressbar.Bar('.', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    print('Collecting Lat and Lon data')
    for i in range(len(database['Url'])):
        bar.update(i+1)
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
            t, itMax = 0, 10
            while t < itMax:
                try:
                    tree = ET.parse(urllib.request.urlopen(url))
                    break
                except:
                    time.sleep(10)
                    t += 1
                    continue
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
    bar.finish()
    for key in dbNew:
        dbNew[key][j + 1:] = []

    return dbNew

def saveBinary(outputName, database):

    print('Saving binary')

    with open(outputName + '.p', 'wb') as outfile:
        pickle.dump(database, outfile)

def saveCsv(outputName, database):

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
    assert sys.argv[-1].lower()  in ['chl', 'frf'], "input argument must be in ['chl', 'frf']"
    if sys.argv[-1].lower() == 'chl':
        server = 'https://chldata.erdc.dren.mil'
    elif sys.argv[-1].lower() == 'frf':
        server = 'http://134.164.129.55'

    main(server)
