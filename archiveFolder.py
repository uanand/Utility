import pandas
import os
import shutil
import zipfile
from zipfile import ZipFile
from time import time

############################################################
# READ THE INPUT EXCEL FILE AND ZIP ARCHIVE THE FOLDERS
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='archiveFolder',names=['inputDir','deleteFlag'])

for inputDir,deleteFlag in df.values:
    archiveFlag = True
    while (archiveFlag==True):
        print ('Archiving the folder %s' %(inputDir))
        tic = time()
        archiveFlag = False
        baseDir = inputDir.split('/')[-1]
        zipFileName = inputDir+'.zip'
        zipObj = ZipFile(zipFileName,'w')
        for root,dirs,files in os.walk(inputDir):
            for fileName in files:
                filePath = os.path.join(root,fileName)
                relPath = baseDir+filePath.split(inputDir)[-1]
                zipObj.write(filePath,arcname=relPath,compress_type=zipfile.ZIP_DEFLATED)
                # zipObj.write(filePath,arcname=relPath)  # UNCOMMENT THIS LINE IF YOU DO NOT WANT ANY COMPRESSION
        zipObj.close()
        
        zipObj = ZipFile(zipFileName,'r')
        ret = zipObj.testzip()
        if (ret is not None):
            print ('Archiving failed. Trying again.')
            archiveFlag=True
        else:
            toc = time()
            print ('Archiving complete. Time taken = %f minutes' %((toc-tic)/60))
        zipObj.close()
        
    # DELETING THE FOLDER IF REQUIRED
    if (deleteFlag==1):
        print ('Deleting the folder %s' %(inputDir))
        shutil.rmtree(inputDir)
############################################################
