from io import StringIO
import pandas as pd
import time
import datetime
import numpy as np
import boto3
import io
import requests
import os
# import s3fs


def readxls_rawprod_convertcsv(bucket, fileName, dateTimeObj):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=fileName)
    file_obj = io.BytesIO(obj['Body'].read())

    newdata = None
    cols_skip = []
    datarawcheck = None

    ###Check excel file
    try:
        datarawcheck = pd.read_excel(file_obj, sheet_name='input', header=None)
        print("bucket:" + bucket + " filename:" + fileName)
        print("Excel Openned")
    except Exception as e:
        print("Can't open the file. Please check the S3 bucket")
        print(str(e))
        return

    #checking is there any blank column we'll tag the position and skip them in next uploading
    for i in range(len(datarawcheck.columns)):
        if (((pd.isna(datarawcheck[i])).nunique()) == 1) & (((
            (pd.isna(datarawcheck[i])).unique())[0]) == True):
            cols_skip.append(i)
        else:
            break

    #checking is there any blank rows for each rows we'll skip it
    rows_skip = []
    for i in range(len(datarawcheck.iloc[i])):
        if (((pd.isna(datarawcheck.iloc[i])).nunique()) == 1) & (((
            (pd.isna(datarawcheck.iloc[i])).unique())[0]) == True):
            rows_skip.append(i)
        else:
            break

    # define unblank columns and rows
    cols = [i for i in range(len(datarawcheck.columns)) if i not in cols_skip]
    rows = len(rows_skip)

    del datarawcheck

    newdata = pd.read_excel(file_obj,
                            sheet_name='input',
                            skiprows=rows,
                            usecols=cols)
    # newdata= pd.read_excel(file_obj, sheet_name='input')
    newdata.reset_index()
    newdata = newdata.dropna(subset=['site'])
    newdata['date'] = pd.to_datetime(newdata['date'])
    print(newdata.head())
    if (newdata is not None or not newdata.empty):
        # newchecking_date = min(newdata['date']) #just to check if user upload file with previous date data
        # flag with timeupload and user upload
        a = str(dateTimeObj)
        b = fileName
        print(a)
        print(b)
        newdata['timesupload'] = a
        newdata['userupload'] = b

        # create new filename
        yr = str(dateTimeObj.year)
        mo = str(dateTimeObj.month)
        day = str(dateTimeObj.day)
        hr = str(dateTimeObj.hour+7)
        mn = str(dateTimeObj.minute)
        sc = str(dateTimeObj.second)
        up_filename = yr + mo + day + hr + mn + sc + '.csv'

        target_bucket = 'abm-data-platform-s3-raw'
        target_object = 'rsw/raw/rawcsv_rsw_ds_rawtest1/' + up_filename
        csv_buffer = StringIO()
        newdata.to_csv(csv_buffer, index=False)
        # s3_resource = boto3.resource('s3',aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(target_bucket,
                           target_object).put(Body=csv_buffer.getvalue())
        client = boto3.client('glue')
        response = client.start_workflow_run(
            Name='abm-rsw-dataingestion-rawtest1'
        )  #groupname-subgroup-dataingestion-table_name
        print('Lambda function is DONE')

    else:
        print("No new data inserted")

    return
