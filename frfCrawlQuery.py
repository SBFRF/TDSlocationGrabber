import csv, pickle
import numpy as np

inputName = 'database'
outputName = 'query'

def query(date, type_, sensor, lat, lon):

    print('Querying')

    with open(inputName + '.p', 'rb') as outfile:
        database = pickle.load(outfile)

    # 1st query = Date

    dateStartList = np.array(database['DateStart'])
    dateEndList = np.array(database['DateEnd'])
    I1 = np.logical_and(dateEndList >= date[0], dateEndList <= date[1])
    I2 = np.logical_and(dateStartList >= date[0], dateStartList <= date[1])
    I3 = np.logical_and(dateStartList <= date[0], dateEndList >= date[1])
    I = np.logical_or(I1, I2)
    I = np.logical_or(I, I3)

    # 2nd query = Type

    typeList = np.array(database['Type'])
    I = np.logical_and(typeList == type_, I)

    # 3rd query = Sensor

    sensorList = np.array(database['Sensor'])
    I = np.logical_and(sensorList == sensor, I)

    # 4th query = lat

    latList = np.array(database['Lat'])
    I_ = np.logical_and(latList >= lat[0], latList <= lat[1])
    I = np.logical_and(I, I_)

    # 5th query = lon

    lonList = np.array(database['Lon'])
    I_ = np.logical_and(lonList >= lon[0], lonList <= lon[1])
    I = np.logical_and(I, I_)

    urlList = np.array(database['Url'])[I]

    allList = [dateStartList[I], dateEndList[I], typeList[I], sensorList[I],
        latList[I], lonList[I], urlList]
    queryData = dict()

    i = 0
    for key in database:
        queryData[key] = allList[i]
        i += 1

    print('Saving query')

    with open(outputName + '.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=queryData.keys())
        writer.writeheader()
        for i in range(len(urlList)):
            row = dict()
            for key in queryData:
                row[key] = queryData[key][i]
            writer.writerow(row)
    
    return queryData

if __name__ == '__main__':
    main()
