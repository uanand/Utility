import pandas
import os
import numpy
import platform

############################################################
# READ THE INPUT EXCEL FILE AND SPLIT THE INPUT FILES INTO BLOCKS OF 128 GB
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='splitFiles',names=['inputFile','outputDir','deleteFlag'])

chunkSizeMB = 1024 # in MB
fileSizeGB = 1 # in GB
zfillVal = 4

chunkSize = chunkSizeMB*1024*1024
fileSize = fileSizeGB*1024*1024*1024
numChunks = int(fileSize/chunkSize)
for inputFile,outputDir,deleteFlag in df.values:
    inputFileSize = os.path.getsize(inputFile)
    numFiles = int(inputFileSize/fileSize)+1
    print ('Splitting the file %s into %d parts' %(inputFile,numFiles))
    
    if (platform.system()=='Linux'):
        fileName = inputFile.split('/')[-1]
    elif (platform.system()=='Windows'):
        fileName = inputFile.split('\\')[-1]
    try:
        if (numpy.isnan(outputDir)):
            outputFile = inputFile
    except:
        outputFile = outputDir+'/'+fileName
        
    chunkNum,splitNum = 0,1
    readFile = open(inputFile,'rb')
    writeFile = open(outputFile+'_split_'+str(splitNum).zfill(zfillVal),'wb')
    
    while (1):
        chunk = readFile.read(chunkSize)
        if not chunk:
            break
        chunkNum = chunkNum+1
        if (chunkNum<=numChunks):
            writeFile.write(chunk)
        else:
            writeFile.close()
            print ('Split number %d saved' %(splitNum))
            splitNum = splitNum+1
            writeFile = open(outputFile+'_split_'+str(splitNum).zfill(zfillVal),'wb')
            writeFile.write(chunk)
            chunkNum = 1
    print ('Split number %d saved' %(splitNum))
    writeFile.close()
    readFile.close()
    
    if (deleteFlag==1):
        os.remove(inputFile)
############################################################
