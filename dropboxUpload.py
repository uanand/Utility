import os
import dropbox
import pandas
import numpy
import platform
from tqdm import tqdm

df = pandas.read_excel('inputs.xlsx',sheet_name='dropboxUpload',names=['inputFile','dropboxFile'])

chunkSizeMB = 64 # in MB
accessToken = '*******************' # GET DROPBOX ACCESS TOKEN BY CREATING AN APP HERE - https://www.dropbox.com/developers/apps

chunkSize = chunkSizeMB*1024*1024
dbx = dropbox.Dropbox(accessToken,timeout=900)

for inputFile,destinationDir in df.values:
    inputFileSize = os.path.getsize(inputFile)
    numChunks = int(numpy.ceil(inputFileSize/chunkSize))
    
    if (platform.system()=='Linux'):
        fileName = inputFile.split('/')[-1]
    elif (platform.system()=='Windows'):
        fileName = inputFile.split('\\')[-1]
    destinationFile = destinationDir+'/'+fileName
    print ('Uploading %s to Dropbox as %s' %(inputFile,destinationFile))
    
    f = open(inputFile,'rb')
    if (numChunks==1):
        dbx.files_upload(f.read(),destinationFile)
    else:
        for i in tqdm(range(numChunks),desc='Number of %d MB chunks uploaded' %(chunkSizeMB)):
            chunk = f.read(chunkSize)
            if (i==0):
                upload_session_start_result = dbx.files_upload_session_start(chunk)
                cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,offset=f.tell())
                commit = dropbox.files.CommitInfo(path=destinationFile)
            elif (i==numChunks-1):
                dbx.files_upload_session_finish(chunk,cursor,commit)
            else:
                dbx.files_upload_session_append(chunk,cursor.session_id,cursor.offset)
                cursor.offset = f.tell()
    f.close()
