#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# @author: Uluc Furkan Vardar
# @updatedDate: 31.10.2019
# @version: 1.0.0
#


from TimeSeries_S3storage import TimeSeries_S3storage

'''
To Store Time Series Data this lib developped.
Storing data with multi source for one time sries is possbile with this lib.

When the data needed lib gives you the concatted data you you want to see.
'''


'''
 Object mst be created with AWS Credantials
 @bucket_name storege bucket in AWS s3 Service
 @prefix the Orjin Folder path of the Stored Data. Defoult  is 'Data/'
 @date_format is the date label format . like 2019-10-31. (use only - or notthing for  splitting date.)
'''
ts_s3S = TimeSeries_S3storage(  AWS_ACCESS_KEY_ID = '...........',
								AWS_SECRET_ACCESS_KEY = '.........',
								bucket_name = '..........',
								prefix = 'Data-test/',
								date_format = '%Y-%m-%d')			



# Sample Data Frame for storing
d = {'Tempature': [12], 'Weight': [ 122.2], 'CPU_usage' : 23.5}
input_dataframe = pd.DataFrame(data=d)


'''
 @input_dataframe is the data that wants to store.
 @data_fileName is the name of the file. For naming multi resource.
 @series_ID is the id that the uniq id  of the series.
 @format file storage format. Lib support parquet file now. Defoult is 'parquet'
 @certain_date is the date the iserted data date label. Defolt is now.
 @logging forprinting file keys.
'''
ts_s3S.put_Data( input_dataframe, data_fileName = 'cpu_info/test1', series_ID = 12, format = 'parquet' ,certain_date = '2019-11-01') # inserted data for given date
ts_s3S.put_Data( input_dataframe, data_fileName = 'cpu_info/test2', series_ID = 12, format = 'parquet' ) # insertd data for now (today date)

'''
 @date for wanted date or dates. like 2019-10-31 means one day
 								 like 2019-10 means all days in the mount of 2019-10
 								 like 2019- means all days in te year 2019
 								 like 201 meas all days in the years starts with 201.*, 2010-2011-.....-2018-2019
 @series_ID is the id that the uniq id  of the series.
 @format file storage format. Lib support parquet file now. Defoult is 'parquet'
 @logging for printing file keys. 								 
'''
print ts_s3S.get_Data(date = '2019-10-31' ,series_ID = 12,loging = True, format = 'parquet')






