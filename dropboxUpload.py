import os
import dropbox
import pandas
import numpy
import platform
from tqdm import tqdm
from time import time
from datetime import datetime

df = pandas.read_excel('inputs.xlsx',sheet_name='dropboxUpload',names=['inputFile','dropboxDir'])

chunkSizeMB = 256 # in MB
accessToken = '**************' # GET DROPBOX ACCESS TOKEN BY CREATING AN APP HERE - https://www.dropbox.com/developers/apps
if (os.path.exists('dropboxUploadLog.dat')):
    logFile = open('dropboxUploadLog.dat','a+')
else:
    logFile = open('dropboxUploadLog.dat','w')
    logFile.write('Start date (YYYYMMDD)\tStart time (HH:MM:SS)\tFile name\tDropbox file name\tUpload status\tFile size (GB)\tUpload time (min)\tUpload speed (MB/s)\n')

chunkSize = chunkSizeMB*1024*1024
dbx = dropbox.Dropbox(accessToken,timeout=900)

inputFileList,destinationDirList,destinationFileList = [],[],[]
for inputFile,destinationDir in df.values:
    inputFileList.append(inputFile)
    if (platform.system()=='Linux'):
        fileName = inputFile.split('/')[-1]
        outputDir = inputFileList.replace('/scratch/utkur','/Backup'); outputDir = outputDir.replace('/'+fileName,'')
        # outputDir = inputFile.replace('/home/utkarsh/Desktop','/Utkarsh'); outputDir = outputDir.replace('/'+fileName,'')
    elif (platform.system()=='Windows'):
        fileName = inputFile.split('\\')[-1]
        outputDir = inputFile.replace('Z:\\','/Backup/');
        outputDir = outputDir.replace('\\','/')
        outputDir = outputDir.replace('/'+fileName,'')
    try:
        if (numpy.isnan(destinationDir)):
            destinationDir = outputDir
    except:
        pass
    destinationDirList.append(destinationDir)
    destinationFile = destinationDir+'/'+fileName
    destinationFileList.append(destinationFile)
destinationDirList = numpy.unique(destinationDirList)

for destinationDir in destinationDirList:
    try:
        dbx.files_create_folder(destinationDir,autorename=False)
    except:
        pass
        
for inputFile,destinationFile in zip(inputFileList,destinationFileList):
    inputFileSize = os.path.getsize(inputFile)
    numChunks = int(numpy.ceil(inputFileSize/chunkSize))
    attemptCounter,successFlag = 1,False
    print ('Uploading %s to Dropbox as %s' %(inputFile,destinationFile))
    
    while (attemptCounter<=2 and successFlag==False):
        try:
            tic = time()
            now = datetime.now(); startDate = now.strftime("%Y%m%d"); startTime = now.strftime("%H:%M:%S")
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
            toc = time()
            status = 'Successful'
            fileSizeGB = inputFileSize/1024/1024/1024
            timeForUpload = (toc-tic)/60
            uploadSpeed = fileSizeGB*1024/(toc-tic)
            logFile.write('%s\t%s\t%s\t%s\t%s\t%f\t%f\t%f\n' %(startDate,startTime,inputFile,destinationFile,status,fileSizeGB,timeForUpload,uploadSpeed))
            successFlag=True
        except:
            toc = time()
            status = 'Failed'
            fileSizeGB = inputFileSize/1024/1024/1024
            timeForUpload = (toc-tic)/60
            uploadSpeed = fileSizeGB*1024/(toc-tic)
            logFile.write('%s\t%s\t%s\t%s\t%s\t%f\t%f\t%f\n' %(startDate,startTime,inputFile,destinationFile,status,fileSizeGB,timeForUpload,uploadSpeed))
            print ("Error uploading %s. Trying again." %(inputFile))
            attemptCounter+=1
            f.close()
logFile.close()
