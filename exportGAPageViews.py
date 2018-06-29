#!/usr/bin/python
# -*- coding: utf-8 -*-

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import calendar as cl
import datetime as dt

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'client_secrets.json'
VIEW_ID = ''

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

def get_report(analytics, date, pageToken = None):
    if pageToken is None:
        return analytics.reports().batchGet(
                body={
                    'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'dateRanges': date,
                        'metrics': [{'expression': 'ga:pageviews'},
                                    {'expression': 'ga:uniquePageviews'},
                                    {'expression': 'ga:timeOnPage'},
                                    {'expression': 'ga:pageLoadTime'}],
                        'dimensions': [{'name': 'ga:dateHourMinute'},
                                       {'name': 'ga:dimension1'}, # UserID
                                       {'name': 'ga:pageTitle'},
                                       {'name': 'ga:pagePath'},
                                       {'name': 'ga:pageDepth'}],
                       'samplingLevel': 'LARGE',
                       'pageSize' : 100000
                   }]
                }
            ).execute()
    else:
        return analytics.reports().batchGet(
                body={
                    'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'dateRanges': date,
                        'metrics': [{'expression': 'ga:pageviews'},
                                    {'expression': 'ga:uniquePageviews'},
                                    {'expression': 'ga:timeOnPage'},
                                    {'expression': 'ga:pageLoadTime'}],
                        'dimensions': [{'name': 'ga:dateHourMinute'},
                                       {'name': 'ga:dimension1'}, # UserID
                                       {'name': 'ga:pageTitle'},
                                       {'name': 'ga:pagePath'},
                                       {'name': 'ga:pageDepth'}],
                       'samplingLevel': 'LARGE',
                       'pageToken' : pageToken,
                       'pageSize' : 100000
                   }]
                }
            ).execute()
    
def reportToList(report):
  list = []
  columnHeader = report.get('columnHeader', {})
  dimensionHeaders = columnHeader.get('dimensions', [])
  metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    
  for row in report.get('data', {}).get('rows', []):
      dict = {}
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        dict[header] = dimension

      for i, values in enumerate(dateRangeValues):
        for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
            if ',' in value or ',' in value:
              dict[metric.get('name')] = float(value)
            else:
              dict[metric.get('name')] = int(value)
      list.append(dict)
  return list

def getGAData(startDate, endDate):
    analytics = initialize_analyticsreporting()
    list = []
    report = get_report(analytics, [{'startDate': startDate, 'endDate': endDate}]).get('reports', [])[0]
    report_data = report.get('data', {})
    print("Got: {} to {}. Row Count: {}".format(startDate, endDate, report_data.get('rowCount')))
    if report_data.get('samplesReadCounts', []) or report_data.get('samplingSpaceSizes', []):
        print("{} to {} contains sampled Data".format(startDate, endDate))
        return 'Sampled Data'
    if report_data.get('rowCount') > 900000:
        print("{} to {} exceeds pagination limit".format(startDate, endDate))
        return 'Exceeded Row Count'
    nextPageToken = report.get('nextPageToken')
    list = reportToList(report)
    while nextPageToken:
        print("\tIterating through pages. Token: {}".format(nextPageToken))
        report = get_report(analytics, [{'startDate': startDate, 'endDate': endDate}], nextPageToken).get('reports', [])[0]
        list = list + reportToList(report)
        nextPageToken = report.get('nextPageToken')
    print("Finalized Segment. Length: {}.".format(len(list)))
    return list
    
    

def getMonthData(year, month):
    lastDay = cl.monthrange(year, month)[1]
    indexDay = 1
    list = []
    while indexDay < lastDay:
        startDate = "{:%Y-%m-%d}".format(dt.datetime(year, month, indexDay))
        indexDay += 3
        if (indexDay > lastDay):
            indexDay = lastDay
        endDate = "{:%Y-%m-%d}".format(dt.datetime(year, month, indexDay))
        while True:
            response = getGAData(startDate, endDate)
            if type(response) != str:
                list = list + response
                break;
            else:
                indexDay -= 1
                endDate = "{:%Y-%m-%d}".format(dt.datetime(year, month, indexDay))
        indexDay += 1
    df = pd.DataFrame(list)
    df.rename(columns={'ga:pageviews' : 'PageViews', 
                       'ga:uniquePageviews' : 'UniquePageViews', 
                       'ga:timeOnPage' : 'TimeOnPage',
                       'ga:pageLoadTime' : 'PageLoadTime',
                       'ga:dateHourMinute' : 'Time',
                       'ga:dimension1' : 'UserID',
                       'ga:pageTitle' : 'PageTitle',
                       'ga:pagePath' : 'PagePath',
                       'ga:pageDepth' : 'PageDepth'}, inplace = True)
    df.reindex(['Time', 'UserID', 'PageTitle', 'PagePath', 'PageDepth', 'UniquePageViews',
                          'PageViews', 'TimeOnPage','PageLoadTime'], axis = 1, inplace = True)
    df['Time'] = pd.to_datetime(df['Time'], errors = 'coerce')
    df = df.dropna()
    df.to_csv("Events-{}-{}.csv".format(year, month), encoding='utf-8', index=False, line_terminator='\n')

months = [(2017, 5),
          (2017, 6),
          (2017, 7),
          (2017, 8)]

for year, month in months:
    getMonthData(year, month)
