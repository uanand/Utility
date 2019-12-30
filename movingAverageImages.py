import os,sys
import numpy
import cv2
import pandas
from tqdm import tqdm
from mpi4py import MPI

import utils

############################################################
# INITIALIZING THE MPI ENVIRONMENT
############################################################
comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank
############################################################

############################################################
# READ THE INPUT EXCEL FILE AND MAKE A LIST OF FRAMES TO AVERAGE
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='movingAverageImages',names=['inputDir','outputDir','firstFrame','lastFrame','averageFrames','extension','movingAverageFlag','normalizeFlag'])

for inputDir,outputDir,firstFrame,lastFrame,averageFrames,extension,movingAverageFlag,normalizeFlag in df.values:
    if (rank == 0):
        try:
            if (numpy.isnan(outputDir)):
                outputDir = inputDir+'_avg'
        except:
            pass
        if (numpy.isnan(firstFrame)):
            firstFrame = 1
        try:
            if (numpy.isnan(extension)):
                extension = 'png'
        except:
            pass
        if (numpy.isnan(movingAverageFlag)):
            movingAverageFlag = 0
        if (numpy.isnan(normalizeFlag)):
            normalizeFlag = 0
    else:
        outputDir = None
        firstFrame = None
        extension = None
        movingAverageFlag = None
        normalizeFlag = None
        
    outputDir = comm.bcast(outputDir,root=0)
    firstFrame = comm.bcast(firstFrame,root=0)
    extension = comm.bcast(extension,root=0)
    movingAverageFlag = comm.bcast(movingAverageFlag,root=0)
    normalizeFlag = comm.bcast(normalizeFlag,root=0)
    
    if (rank==0):
        utils.mkdir(outputDir)
        print ("Averaging images from %s" %(inputDir))
    avgFrameList = []
    if (movingAverageFlag==0):
        for frame1 in range(firstFrame,lastFrame+1,averageFrames):
            if (frame1+averageFrames<=lastFrame+1):
                frameList = []
                for frame2 in range(frame1,frame1+averageFrames):
                    frameList.append(frame2)
                avgFrameList.append(frameList)
    elif (movingAverageFlag==1):
        for frame1 in range(firstFrame,lastFrame+1):
            if (frame1+averageFrames<=lastFrame+1):
                frameList = []
                for frame2 in range(frame1,frame1+averageFrames):
                    frameList.append(frame2)
                avgFrameList.append(frameList)
    procAvgFrameList = numpy.array_split(avgFrameList,size)
    comm.barrier()
    
    if (extension=='png'):
        for frameList in tqdm(procAvgFrameList[rank]):
            outputFile = outputDir+'/'+str(frameList[0]).zfill(6)+'.png'
            for frame in frameList:
                inputFile = inputDir+'/'+str(frame).zfill(6)+'.png'
                gImg = cv2.imread(inputFile,0)
                if (frame==frameList[0]):
                    avgImg = gImg.copy()
                    avgImg = avgImg.astype('double')
                else:
                    avgImg = avgImg+gImg
            avgImg = (avgImg/averageFrames).astype('uint8')
            if (normalizeFlag==1):
                avgImg = utils.normalize(avgImg)
            cv2.imwrite(outputFile,avgImg)
    elif (extension=='npy'):
        for frameList in tqdm(procAvgFrameList[rank]):
            outputFile = outputDir+'/'+str(frameList[0]).zfill(6)+'.png'
            for frame in frameList:
                inputFile = inputDir+'/'+str(frame).zfill(6)+'.npy'
                gImg = numpy.load(inputFile)
                if (frame==frameList[0]):
                    avgImg = gImg.copy()
                    avgImg = avgImg.astype('double')
                else:
                    avgImg = avgImg+gImg
            avgImg = avgImg/averageFrames
            avgImg = utils.normalize(avgImg)
            cv2.imwrite(outputFile,avgImg)
    elif (extension=='tif'):
        for frameList in tqdm(procAvgFrameList[rank]):
            outputFile = outputDir+'/'+str(frameList[0]).zfill(6)+'.tif'
            for frame in frameList:
                inputFile = inputDir+'/'+str(frame).zfill(6)+'.tif'
                gImg = cv2.imread(inputFile,-1)
                if (frame==frameList[0]):
                    avgImg = gImg.copy()
                    avgImg = avgImg.astype('double')
                else:
                    avgImg = avgImg+gImg
            avgImg = (avgImg/averageFrames).astype('uint16')
            if (normalizeFlag==1):
                avgImg = utils.normalize(avgImg)
            cv2.imwrite(outputFile,avgImg)
comm.barrier()
############################################################
