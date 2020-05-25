import pandas
import os
from tqdm import tqdm
import utils

############################################################
# READ THE INPUT EXCEL FILE AND COMBINE THE SPLIT FILES
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='joinFiles',names=['inputFile','deleteFlag'])

chunkSizeMB = 1024 # in MB
chunkSize = chunkSizeMB*1024*1024

for inputFile,deleteFlag in df.values:
    fileName = inputFile.split('_split_0001')[0]
    splitFileList = utils.findSplitFiles(inputFile)
    print ('Combining split files for %s' %(fileName))
    outFile = open(fileName,'wb')
    for splitFile in tqdm(splitFileList):
        inFile = open(splitFile,'rb')
        while (1):
            chunk = inFile.read(chunkSize)
            if not chunk:
                break
            outFile.write(chunk)
        inFile.close()
        if (deleteFlag==1):
            os.remove(splitFile)
    outFile.close()
############################################################
