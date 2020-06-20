**GDrive_ImageLoad.py**


This python script downloads csv files from an S3 bucket, downloads images from url links in this file, and creates/loads these images to a Google Drive folder based on date.

*To run this script ensure you have:*

created a GCP app

client_secrets.json -- A downloaded file with the authentication information for your application on Google Cloud Platform.

mycreds.txt -- The access token for the Google API, otherwise you will be prompted to authenticate the app in your web browser
