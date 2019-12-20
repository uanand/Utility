import os
import numpy
import pandas
from tqdm import tqdm
from mpi4py import MPI

############################################################
# INITIALIZING THE MPI ENVIRONMENT
############################################################
comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank
############################################################

############################################################
# READ THE INPUT EXCEL FILE AND REMOVE FILES FROM DIRECTORIES
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='deleteImageSequence',names=['inputDir','firstFrame','lastFrame','extension'])

for inputDir,firstFrame,lastFrame,extension in df.values:
    if (rank==0):
        print ('Deleting files from', inputDir)
    try:
        if (numpy.isnan(extension)):
            extension = '.png'
    except:
        pass
    frameList = range(firstFrame,lastFrame+1)
    procFrameList = numpy.array_split(frameList,size)
    comm.barrier()
    
    for frame in tqdm(procFrameList[rank]):
        fileName = inputDir + '/' + str(frame).zfill(6) + extension
        if (os.path.exists(fileName)):
            os.remove(fileName)
    comm.barrier()
############################################################
