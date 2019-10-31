#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# @author: Uluc Furkan Vardar
# @updatedDate: 31.10.2019
# @version: 1.0.0
#
import datetime
import pandas as pd
import boto3
import json
import uuid
import os
from io import BytesIO


class TimeSeries_S3storage:
	def __init__(self, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, bucket_name , prefix = 'Data/', date_format = '%Y-%m-%d' ):
		os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
		os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
		self.bucket_name = bucket_name
		self.client = boto3.client('s3')
		self.resource = boto3.resource('s3')
		self.prefix = prefix
		self.date_format = date_format

	def put_Data(self, input_dataframe, data_fileName, series_ID, format = 'parquet', loging = False, certain_date = None):
		file_format = ''
		if format == 'parquet':
			out_buffer = BytesIO()
			input_dataframe.to_parquet(out_buffer, index=False)

		elif format == 'csv':
			out_buffer = StringIO()
			input_dataframe.to_parquet(out_buffer, index=False)
		else:
			print (' ERROR : Data format is not  true')
			return None

		if certain_date !=None:
			file_date = certain_date
		else:
			file_date = datetime.date.today().strftime(self.date_format)
		
		# file naming 
		randomSTR = str(uuid.uuid4())
		filepath = '%s%s/%s/%s--%s.%s'%( self.prefix, str(series_ID), file_date, data_fileName, randomSTR, format )


		if loging == True:
			print ('Bucket : ',self.bucket_name)
			print ('File_Key : ', filepath)


		try:
			self.client.put_object(Bucket=self.bucket_name, Key=filepath, Body=out_buffer.getvalue())    	
		except Exception as e:
			raise Exception("\n\n *AWS* Error is occured when uploading Data " + filepath + " **\n" + str(e))

	def get_dataPaths(self, date ,series_ID = None, format = 'parquet'):
		filepath = '%s%s/%s'%( self.prefix, str(series_ID), date)
		objects_path = [k.key for k in self.resource.Bucket( self.bucket_name ).objects.filter(Prefix = filepath)  if k.key.endswith(format)]
		return objects_path

	def download_s3_parquet_file(self, bucket, key):
		buffer = BytesIO()
		self.resource.Object(bucket,key).download_fileobj(buffer)
		df = pd.read_parquet(buffer)
		df['file'] = key
		df['date'] = key.split('/')[2]
		return df

	def get_Data(self, date ,series_ID = None,loging = False, format = 'parquet'):

		s3_keys = self.get_dataPaths( date, series_ID , format )
		if loging == True:
			print s3_keys

		dfs = [self.download_s3_parquet_file(self.bucket_name, k) for k in s3_keys]
		df = pd.concat(dfs,sort=True , keys=None)	

		# Date ordering
		#cols = df.columns.tolist()	
		#cols.remove('date')
		#3df = df[ ['date'] + cols ]

		return df