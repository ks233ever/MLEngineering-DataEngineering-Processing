import paramiko
import datetime
import sys
import os
import boto3
import pandas as pd
import numpy as np
from zipfile import ZipFile
import seaborn as sns
import matplotlib.pyplot as plt


class ProcessFiles:

    def __init__(self, label, path):
        self.label = label
        self.path = path
        self.df_final = None

    def ftp_download(self):
        """Download zip files from a specified FTP directory and save them locally"""

        os.mkdir(self.path + '/{}'.format(self.label))

        transport = paramiko.Transport(('XXXXXXXX', 22))
        transport.connect(username='XXXXX', password='XXXXX')
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.chdir(path='XXXXXXX/Out/')

        downloads = []

        for filename in sorted(sftp.listdir()):
            sftp.get(filename, self.path + '/{}/{}.zip'.format(self.label, filename))
            downloads.append(filename)

        sftp.close()

        print('Downloaded the following files into your path: {}'.format(downloads))

    def s3_upload(self):
        """Extract local zip files and upload them to an S3 bucket"""

        session = boto3.Session()
        # low-level client interfact
        s3_client = session.client('s3')
        # high level interface
        s3_resource = session.resource('s3')

        bucket = s3_resource.Bucket('XXXXXX')
        print('The current directories in your S3 bucket are: ')
        for obj in bucket.objects.all():
            print(obj.key)

        print('\n')

        rootdir = self.path + '/{}/'.format(self.label)

        # extract the zip files
        files_paths = []
        for subdir, dirs, files in os.walk(rootdir):
            print('Grabbing the downloaded zip file paths to extract: ')
            for file in files:
                if '.csv' not in file:
                    ex_path = os.path.join(subdir, file)
                    print(ex_path)
                    files_paths.append(ex_path)

        print('total files to extract: ', len(files_paths))
        print('\n')

        # creating a list of extracted file names that we will upload to s3
        s3_files = []
        for i in files_paths:

            # Create a ZipFile Object
            with ZipFile(i, 'r') as zipObj:
                # Get a list of all archived file names from the zip
                listOfFileNames = zipObj.namelist()
                # Iterate over the file names
                for fileName in listOfFileNames:
                 # Check filename doesn't have restriction in it
                    if 'restriction' not in fileName:
                        # Extract the single file from zip to upload to s3
                        zipObj.extract(fileName, self.path + '/{}/Unzipped'.format(self.label))
                        s3_files.append(fileName)

        print('We have {} extracted files to upload to the S3 bucket:'.format(len(s3_files)))
        print(s3_files)
        print('\n')

        for f in s3_files:
            filepath = self.path + '/{}/Unzipped/'.format(self.label) + f
            s3_resource.Bucket('XXXXXX').upload_file(
                Filename=filepath, Key='files/files/{}/{}'.format(self.label, f))

        bucket = s3_resource.Bucket('XXXXXX')
        print('files in the S3 bucket now include: ')
        for obj in bucket.objects.all():
            print(obj.key)
        print('\n')

    def s3_download(self):
        """Download S3 files locally"""

        session = boto3.Session()
        # low-level client interfact
        s3_client = session.client('s3')
        # high level interface
        s3_resource = session.resource('s3')

        bucket = s3_resource.Bucket('XXXXXX')

        recent_files = []
        print('The following {} files will be downloaded into an S3_downloads folder within your path: '.format(self.label))
        for obj in bucket.objects.filter(Prefix='files/files/{}'.format(self.label)):
            recent_files_files.append(obj.key)
            print(obj.key)
        print('\n')

        # creating the new directory to download into
        os.mkdir(self.path + '/{}/S3_downloads'.format(self.label))

        for f in recent_files_files:
            filename = ''.join(f.split('/')[-1:])
            s3_resource.Bucket('XXXXXXXX').download_file(f, self.path + '/{}/S3_downloads/{}'.format(self.label, filename))
        print('Download complete!')
