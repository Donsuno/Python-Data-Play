import json
from datetime import datetime
from rawprod_to_csv import readxls_rawprod_convertcsv

# import ptvsd

# ptvsd.enable_attach(address=('0.0.0.0',5890),redirect_output=True)
# ptvsd.wait_for_attach()

def lambda_handler(event, context):

    if event:
        dateTimeObj = datetime.now() + timedelta(hours=1)
        file_obj=event["Records"][0]
        fileName=str(file_obj['s3']['object']['key']).replace("+"," ")
        bucket = str(file_obj['s3']['bucket']['name']).replace("+"," ")
        
        try:
            x=fileName
            if x=='rsw/uploadhere-user1/rawtest1.xlsx' :
                print(x)
                readxls_rawprod_convertcsv(bucket, fileName, dateTimeObj)
                print("go to code: readxls_rawprod_convertcsv is passed")
            else:
                print("the uploaded format file is not sufficient")

        except Exception as e:
            print(str(e))
            return {
            'statusCode': 200,
            'body': json.dumps(str(e))}
