import datetime
import sys
import os
import boto3
import pandas as pd
from zipfile import ZipFile
import logging
import json
import io
import shutil
from smart_open import smart_open
from io import BytesIO
from PIL import Image
import time
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from collections import Counter
import urllib
from urllib.request import urlretrieve
import requests
from PIL import Image
from keras.preprocessing import image
import numpy as np

from sagemaker import get_execution_role
from sagemaker import tensorflow
import re


# Grab CSV files from S3 bucket


session = boto3.Session(region_name='us-east-1')
# low-level client interfact
s3_client = session.client('s3')
# high level interface
s3_resource = session.resource('s3')

bucket = s3_resource.Bucket('bucket')
obj_names = []
for obj in bucket.objects.filter(Prefix='path/'):
    obj_name = obj.key
    if 'csv' in obj_name:
        print(obj_name)
        logging.basicConfig(filename='logfile_path/logfile_{}.txt'.format(re.sub('[^0-9]', '-', str(datetime.datetime.now()))), level=logging.INFO)
        logging.info(obj_name)
        obj_names.append(obj_name)


dfs = []

# for loop to load up multiple files

for i in obj_names:
    df_new = pd.read_csv(smart_open('s3://bucket/{}'.format(i)), error_bad_lines=False)
    dfs.append(df_new)

df = pd.concat(dfs)


logging.info('All records')
logging.info(df.shape)

# grab unique dates

logging.info('\n')
logging.info('The unique DateStamps are {}'.format(df['DateStamp'].unique()))


# Create Gdrive Directories


logging.info('Connecting the Gdrive API')
gauth = GoogleAuth()
# Try to load saved client credentials
gauth.LoadCredentialsFile("path/mycreds.txt")
if gauth.credentials is None:
    # Authenticate if they're not there
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    # Refresh them if expired
    gauth.Refresh()
else:
    # Initialize the saved creds
    gauth.Authorize()
# Save the current credentials to a file
gauth.SaveCredentialsFile("path/mycreds.txt")


drive = GoogleDrive(gauth)
logging.info('Successfully connected')


logging.info('Creating the appropriate directories in Gdrive')


file_list = drive.ListFile({'q': "'file_id' in parents and trashed=false"}).GetList()

# creating a dictionary of all current DateStamps and their id's

current_dirs = {}

for file1 in file_list:
    current_dirs['{}'.format(file1['title'])] = file1['id']


target_load = {}


for DateStamp in df['DateStamp'].unique():

    DateStamp = str(DateStamp)

    # create the DateStamp folder if not already there

    if DateStamp not in current_dirs:

        folder_metadata = {'title': str(DateStamp), 'mimeType': 'application/vnd.google-apps.folder',
                           'parents': [{"kind": "drive#fileLink", "id": 'file_id'}]}

        folder = drive.CreateFile(folder_metadata)
        folder.Upload()

        folderid = folder['id']

        target_load[DateStamp] = folderid

    else:

        # grab a count of how many x the DateStamp appears
        # create new folder as DateStamp + count exists + 1

        l = []

        for file1 in file_list:
            l.append(file1['title'].split('_')[0])

        counts = Counter(l)

        folder_metadata = {'title': DateStamp + '_' + str(counts[DateStamp] + 1), 'mimeType': 'application/vnd.google-apps.folder',
                           'parents': [{"kind": "drive#fileLink", "id": 'file_id'}]}

        folder = drive.CreateFile(folder_metadata)
        folder.Upload()

        folderid = folder['id']

        target_load[DateStamp] = folderid


logging.info('Directories have been created!')


logging.info('\n')


# Download images and upload to appropriate directories


logging.info('Downloading images')

errors = []
count = 0

images_dates = list(zip(df.ImageLink, df.DateStamp))

for i, d in images_dates:
    count += 1

    try:
        urlretrieve('{}'.format(i), "path/{}.jpg".format(count))

        with open("path/{}.jpg".format(count), "r") as file:
            file_drive = drive.CreateFile({'title': str(count) + '_' + count,
                                           "parents": [{"kind": "drive#fileLink", "id": target_load[DateStamp]}]})
            file_drive.SetContentFile("path/{}.jpg".format(count))
            file_drive.Upload()

    except:
        errors.append((i, count))
        logging.info('\n')
        logging.info(count)
        logging.info('error downloading image')
        logging.info('\n')
        # print(count)
        pass


# remove images from local path so as not to clutter

logging.info('\n')
for count, i in images_dates:
    try:

        os.remove("path/{}.jpg".format(count))
        logging.info('removed ' + count)

    except:
        pass


# Upload any Image Link Errors

# reconnect

logging.info('Reconnecting to Gdrive API')
gauth = GoogleAuth()
# Try to load saved client credentials
gauth.LoadCredentialsFile("path/mycreds.txt")
if gauth.credentials is None:
    # Authenticate if they're not there
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    # Refresh them if expired
    gauth.Refresh()
else:
    # Initialize the saved creds
    gauth.Authorize()
# Save the current credentials to a file
gauth.SaveCredentialsFile("path/mycreds.txt")


drive = GoogleDrive(gauth)
logging.info('Successfully connected')


if len(errors) > 0:

    logging.info('\n')
    logging.info('some image links errored out, uploading cases to Gdrive')

    er = pd.DataFrame(errors, columns=['link', 'names'])
    er.to_excel('path/errors.xlsx', index=False)

    folder_metadata = {'title': 'image link errors', 'mimeType': 'application/vnd.google-apps.folder',
                       'parents': [{"kind": "drive#fileLink", "id": folderid}]}

    file_drive = drive.CreateFile(folder_metadata)
    file_drive.Upload()

    file_drive2 = drive.CreateFile({'title': 'image link errors',
                                    "parents": [{"kind": "drive#fileLink", "id": file_drive['id']}]})
    file_drive2.SetContentFile('path/errors.xlsx')
    file_drive2.Upload()
    logging.info('Successfully uploaded the image errors')
else:
    logging.info('No image link errors to upload')
