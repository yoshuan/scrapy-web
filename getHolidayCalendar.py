# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 11:05:36 2017

@author: HP
"""
#Crawl and download opendata
#Upload to aws s3 bucket
#Save Calendar Link into dynamoDB

import urllib
import requests
import re
import shutil
import boto3
import pandas as pd
import os

os.chdir('''YOUR WORKING DIR''')


URL = 'http://www.calendarlabs.com/ical-calendar-holidays.php'
BUCKET = 'holidayBucket'
BUCKET_URL = 'https://s3.amazonaws.com/holidayBucket/'
TABLE = 'holidayCalendar'
LOCAL_PATH = 'LOCAL DIR TO STORE DOWNLOADED FILES'
ISO_CSV = 'iso_3166_2_countries.csv'

s3 = boto3.resource('s3')
s3.create_bucket(Bucket = BUCKET)

dynamodb = boto3.resource('dynamodb')
s3LinkTable = dynamodb.Table(TABLE)

def getHtmlfromPage(url):
    text = urllib.urlopen(url).read()
    return text

def parseLink(regx, text):
    link = re.findall(regx, text)
    calLinks = []
    for i in range(len(link)):
        calLinks.append('http://' + link[i])
    return calLinks
 
def downloadCal(linklist, path):
    for url in linklist:
        filename = re.findall('ical/(.+)', url)[0]
        with open(path + filename, 'wb') as f:
            res = requests.get(url, stream=True)
            shutil.copyfileobj(res.raw, f)

def uploadCal(localfile, linklist, bucket):
    client = boto3.client('s3')
    for url in linklist:
        filename = re.findall('ical/(.+)', url)[0]
        client.put_object(ACL= 'public-read',
                          Bucket = bucket,
                          Key = filename,
                          Body = localfile + filename)

def getCalLinkfromHtml(html):
    calLinks = parseLink('<li><a href="webcal://(.+)"><img', html)
    calLinks.extend(parseLink('<li><a href="webcal://(.+)"> <img', html))
    return calLinks

def printCalendarName(linklist):
    calendarName = list()
    for url in linklist:
        calendarName.append(re.findall('ical/(.+)-Holidays.ics', url)[0])       
    return pd.DataFrame(calendarName, columns=['Common Name'])

def mappingCodeWithCalendarName(calLink, countryCodeDf):
    calendarName = printCalendarName(calLinks)
    calendars = pd.merge(calendarName, countryCodeDf, how = 'left', on = ['Common Name'])
    for i in range(len(calendars)):
        if type(calendars.iloc[i,1]) == float:
                calendars.iloc[i,1] = calendars.iloc[i,0]
    return calendars

def createS3Link(calendars):
    calendars['URL'] = 'URL'
    for i in range(len(calendars)):
        calendars.iloc[i,2] = BUCKET_URL + calendars.iloc[i,0] + '-Holidays.ics'
    return calendars
    
def tablewriter(table, calendars):
    with table.batch_writer() as batch:
        for i in range(len(calendars)):
            batch.put_item(
                            Item={
                                'calendarCode': calendars.iloc[i,1],
                                'calendarURI': calendars.iloc[i,2]
                                }
                            )    

if __name__=='__main__':
    #Prepare for Downloading .ics Files
    html = getHtmlfromPage(URL)
    calLinks = getCalLinkfromHtml(html)
    #Download and Upload Files to s3
    downloadCal(calLinks, LOCAL_PATH)
    uploadCal(LOCAL_PATH, calLinks, BUCKET)
    #Map Country ISO Code With Country Name
    countryCodeDf = pd.read_csv(ISO_CSV).ix[:,[1,11]]
    calendars = mappingCodeWithCalendarName(calLinks, countryCodeDf)
    calendars = createS3Link(calendars)
    #Write Calendars Link into DynamoDB
    tablewriter(s3LinkTable, calendars)