import os
import cv2
import numpy
import pandas
import datetime
import hyperspy.api as hs
from tqdm import tqdm
from mpi4py import MPI

import utils

EPOCH_AS_FILETIME = 116444736000000000
HUNDREDS_OF_NANOSECONDS = 10000000

############################################################
# INITIALIZING THE MPI ENVIRONMENT
############################################################
comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank
############################################################

############################################################
# READ THE INPUT EXCEL FILE AND CONVERT MOVIE
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='convertGatanDM4Movies',names=['inputDir','outputPNGDir','outputPNGFlag','outputNPYFlag','scalingFactor','minPercentile','maxPercentile','averageFrames','firstFrame','lastFrame'])

if (rank==0):
    logFile = open('convertGatanDM4Movies.log','w')
    logFile.write("InputDir\tFileName\tFrame\tCount\tPixel size (nm)\tFOV (nm)\tDose (e/(A2s))\tYear\tMonth\tDay\tHour\tMinute\tSecond\tMicrosecond\n")
    
for inputDir,outputDir,outputPNGFlag,outputNPYFlag,scalingFactor,minPercentile,maxPercentile,averageFrames,firstFrame,lastFrame in df.values:
    if (rank == 0):
        try:
            if (numpy.isnan(outputDir)):
                outputDir = inputDir+'_converted'
                utils.mkdir(outputDir)
        except:
            pass
        outputPNGDir = outputDir+'/png'
        outputNPYDir = outputDir+'/npy'
        if (numpy.isnan(outputPNGFlag) or int(outputPNGFlag)==1):
            outputPNGFlag = 1
            utils.mkdir(outputPNGDir)
        if (numpy.isnan(outputNPYFlag)):
            outputNPYFlag = 0
        elif (int(outputNPYFlag)==1):
            utils.mkdir(outputNPYDir)
            outputNPYFlag = 1
        else:
            outputNPYFlag = 0
        if (numpy.isnan(scalingFactor)):
            scalingFactor = 1
        if (numpy.isnan(minPercentile)):
            minPercentile = 0.1
        if (numpy.isnan(maxPercentile)):
            maxPercentile = 99.9
        if (numpy.isnan(firstFrame)):
            firstFrame = 1
        if (numpy.isnan(lastFrame)):
            lastFrame = 1e10
        if (numpy.isnan(averageFrames)):
            averageFrames = 1
    else:
        outputDir = None
        outputPNGFlag = None
        outputPNGDir = None
        outputNPYFlag = None
        outputNPYDir = None
        scalingFactor = None
        minPercentile = None
        maxPercentile = None
        firstFrame = None
        lastFrame = None
        averageFrames = None
            
    outputDir = comm.bcast(outputDir,root=0)
    outputPNGFlag = comm.bcast(outputPNGFlag,root=0)
    outputPNGDir = comm.bcast(outputPNGDir,root=0)
    outputNPYFlag = comm.bcast(outputNPYFlag,root=0)
    outputNPYDir = comm.bcast(outputNPYDir,root=0)
    scalingFactor = comm.bcast(scalingFactor,root=0)
    minPercentile = comm.bcast(minPercentile,root=0)
    maxPercentile = comm.bcast(maxPercentile,root=0)
    firstFrame = comm.bcast(firstFrame,root=0)
    lastFrame = comm.bcast(lastFrame,root=0)
    averageFrames = comm.bcast(averageFrames,root=0)
    
    if (rank==0):
        print ('Processing image sequence %s' %(inputDir))
        inputFileList = utils.getFiles(inputDir)
    else:
        inputFileList = None
    comm.barrier()
    
    inputDirList = []
    frameDM4List = []
    inputDM4FileList = []
    outputPNGFileList = []
    outputPNGFlagList = []
    outputNPYFileList = []
    outputNPYFlagList = []
    scaleList = []
    minPercentileList = []
    maxPercentileList = []
    averageFramesList = []
    inputFileList = comm.bcast(inputFileList,root=0)
    numFrames = inputFileList.size
    frameList = range(1,numFrames+1)
    for inputFile,frame in zip(inputFileList,frameList):
        if (frame>=firstFrame and frame<=lastFrame):
            inputDirList.append(inputDir)
            frameDM4List.append(frame)
            inputDM4FileList.append(inputFile)
            outputPNGFileList.append(outputPNGDir+'/'+str(frame).zfill(6)+'.png')
            outputPNGFlagList.append(outputPNGFlag)
            outputNPYFileList.append(outputNPYDir+'/'+str(frame).zfill(6)+'.npy')
            outputNPYFlagList.append(outputNPYFlag)
            scaleList.append(scalingFactor)
            minPercentileList.append(minPercentile)
            maxPercentileList.append(maxPercentile)
            averageFramesList.append(averageFrames)
    fileInfo = numpy.column_stack((inputDirList,frameDM4List,inputDM4FileList,outputPNGFileList,outputPNGFlagList,outputNPYFileList,outputNPYFlagList,scaleList,minPercentileList,maxPercentileList,averageFramesList))
    fileInfoProc = numpy.array_split(fileInfo,size)
    
    ############################################################
    # FIRST PASS THROUGH ALL FILES TO GET THE CONTRAST VALUE
    outFile = open(str(rank)+'.dat','w')
    outFileCounts = open('counts_'+str(rank)+'.dat','w')
    for inputDir,frame,inputFile,outputPNGFile,outputPNGFlag,outputNPYFile,outputNPYFlag,scale,minPercentile,maxPercentile,averageFrames in tqdm(fileInfoProc[rank]):
        try:
            f = hs.load(inputFile)
            gImg = f.data
            [row,col] = gImg.shape
            
            ############################################################
            # CALCULATING THE TIME STAMP FOR EACH FRAME
            windowsSystemTime = f.original_metadata['ImageList']['TagGroup0']['ImageTags']['DataBar']['Acquisition Time (OS)']
            dateTime = datetime.datetime.fromtimestamp((windowsSystemTime-EPOCH_AS_FILETIME)/HUNDREDS_OF_NANOSECONDS)
            
            ############################################################
            # CALCULATING THE DOSE FOR FRAME AND WRITING INTO A TEXT FILE
            pixInNM = f.original_metadata['ImageList']['TagGroup0']['ImageData']['Calibrations']['Dimension']['TagGroup0']['Scale']
            scaleUnit = f.original_metadata['ImageList']['TagGroup0']['ImageData']['Calibrations']['Dimension']['TagGroup0']['Units']
            cameraName = f.original_metadata['ImageList']['TagGroup0']['ImageTags']['Acquisition']['Device']['Name']
            exposureTime = f.original_metadata['ImageList']['TagGroup0']['ImageTags']['Acquisition']['Parameters']['Detector']['exposure (s)']
            try:
                lowLimit = f.original_metadata['DocumentObjectList']['TagGroup0']['ImageDisplayInfo']['LowLimit']
                highLimit = f.original_metadata['DocumentObjectList']['TagGroup0']['ImageDisplayInfo']['HighLimit']
            except:
                lowLimit,highLimit = numpy.percentile(gImg,float(minPercentile)),numpy.percentile(gImg,float(maxPercentile))
                
            fps = 1./exposureTime
            if (scaleUnit==u'\xb5m'):
                pixInNM = pixInNM*1000
            pixInAng = pixInNM*10
            FOV = pixInNM*col
            counts = numpy.mean(gImg)
            if (cameraName == u'OneView'):
                N = 38.0
            elif (cameraName == u'K2-IS-0001'):
                N = 0.8
            flux = counts/N*fps/pixInAng**2
            outFile.write("%s\t%s\t%d\t%f\t%f\t%f\t%f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" %(inputDir,inputFile,int(frame),counts,pixInNM,FOV,flux,dateTime.year,dateTime.month,dateTime.day,dateTime.hour,dateTime.minute,dateTime.second,dateTime.microsecond))
            outFileCounts.write('%f\t%f\n' %(lowLimit,highLimit))
        except:
            print ('%s is corrupt. Could not read!' %(inputFile))
            outFile.write("%s\t%s\t%d\tnan\tnan\tnan\tnan\tnan\tnan\tnan\tnan\tnan\tnan\tnan\n" %(inputDir,inputFile,int(frame)))
            outFileCounts.write('nan\tnan\n')
    outFile.close()
    outFileCounts.close()
    comm.barrier()
    
    ############################################################
    # COMPILE INFORMATION FROM LOG FILES AND CONTRAST FILES
    if (rank==0):
        for r in range(size):
            inFile = open(str(r)+'.dat','r')
            for line in inFile:
                logFile.write(line)
            os.remove(str(r)+'.dat')
            
        outFileCounts = open('counts.dat','w')
        for r in range(size):
            inFile = open('counts_'+str(r)+'.dat','r')
            for line in inFile:
                outFileCounts.write(line)
            os.remove('counts_'+str(r)+'.dat')
        outFileCounts.close()
    comm.barrier()
    
    ############################################################
    # SECOND PASS THROUGH ALL FILES TO SAVE IMAGES WITH APPROPIATE CONTRAST
    counts = numpy.loadtxt('counts.dat',delimiter='\t')
    minCount,maxCount = numpy.min(counts[:,0]),numpy.max(counts[:,1])
    if (rank==0):
        os.remove('counts.dat')
    comm.barrier()
    for inputDir,frame,inputFile,outputPNGFile,outputPNGFlag,outputNPYFile,outputNPYFlag,scale,minPercentile,maxPercentile,averageFrames in tqdm(fileInfoProc[rank]):
        try:
            f = hs.load(inputFile)
            gImg = f.data
            [row,col] = gImg.shape
            
            if (int(outputNPYFlag)==1):
                numpy.save(outputNPYFile,gImg)
                
            try:
                lowLimit = f.original_metadata['DocumentObjectList']['TagGroup0']['ImageDisplayInfo']['LowLimit']
                highLimit = f.original_metadata['DocumentObjectList']['TagGroup0']['ImageDisplayInfo']['HighLimit']
            except:
                lowLimit,highLimit = numpy.percentile(gImg,float(minPercentile)),numpy.percentile(gImg,float(maxPercentile))
            gImg[gImg<=lowLimit] = lowLimit
            gImg[gImg>=highLimit] = highLimit

            if (int(outputPNGFlag)==1):
                if (float(scale)<1):
                    gImg = cv2.resize(gImg,(int(col*float(scale)),int(row*float(scale))),interpolation=cv2.INTER_AREA)
                elif (float(scale)>1):
                    gImg = cv2.resize(gImg,(int(col*float(scale)),int(row*float(scale))),interpolation=cv2.INTER_LINEAR)
                gImg = utils.normalize(gImg,min=(gImg.min()-minCount)/(maxCount-minCount)*255,max=(gImg.max()-minCount)/(maxCount-minCount)*255)
                cv2.imwrite(outputPNGFile,gImg)
        except:
            pass
    comm.barrier()
logFile.close()
############################################################
