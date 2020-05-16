
import os


def WriteCsvtoXml():
    csvSrcpath = "/Users/tanvi/PycharmProjects/assignment/data/processed/Success/"
    xmlOutputPath = "/Users/tanvi/PycharmProjects/assignment/data/processed/xml/"
    file_list = os.listdir (csvSrcpath)
    #os.chdir(csvSrcpath)
    print (file_list)
    tags = ['intdata', 'intdata1', 'date', 'decimldata', 'string', 'datetime']

    for csvFileName in file_list:
        xmlFile = csvFileName[:-4] + '.xml'
        print('csv file name : ', csvFileName)
        csvFile = open (csvSrcpath+csvFileName, 'r')
        csvData = csvFile.readlines()
        csvFile.close ()
        xmlData = open (xmlOutputPath+xmlFile, 'w')
        xmlData.write ('<?xml version="1.0"?>' + "\n")
        # there must be only one top-level tag
        xmlData.write ('<csv_data>' + "\n")

        for row in csvData:
             delimiter=","
             rowData = row.strip ().split (delimiter)
             xmlData.write ('<row>' + "\n")
             for i in range (len (tags)):
               xmlData.write ('  ' + '<' + tags[i] + '>' + rowData[i] + '</' + tags[i] + '>' + "\n")
             xmlData.write ('</row>' + "\n")
        xmlData.write ('</csv_data>' + "\n")
        xmlData.close ()

WriteCsvtoXml()