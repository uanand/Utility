import pandas
import os
from tqdm import tqdm
import utils

# def findSplitFiles(inputFile):
    # if (platform.system()=='Linux'):
        # tempList = inputFile.split('/')
        # path = ''
        # for temp in tempList[:-1]:
            # path = path+temp+'/'
    # elif (platform.system()=='Windows'):
        # tempList = inputFile.split('\\')
        # path = ''
        # for temp in tempList[:-1]:
            # path = path+temp+'\\'
    # fileName = inputFile.split('_split_0001')[0]+'_split_'
    # dirFileList = listdir(path)
    # splitFileList = []
    # for dirFile in dirFileList:
        # if (fileName in path+dirFile):
            # splitFileList.append(path+dirFile)
    # splitFileList = list(numpy.sort(splitFileList,kind='mergesort'))
    # return splitFileList
    
############################################################
# READ THE INPUT EXCEL FILE AND SPLIT THE INPUT FILES INTO BLOCKS OF 128 GB
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='joinFiles',names=['inputFile','deleteFlag'])

chunkSizeMB = 64 # in MB
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
