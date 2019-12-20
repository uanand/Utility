# TODO - FIGURE OUT A WAY TO READ THE PIXEL SIZE FROM THE SEQ FILE METADATA

import numpy
import pims
import mrcz
import pandas
import cv2
import os
import gc
from tqdm import tqdm
from mpi4py import MPI

import utils

############################################################
# INITIALIZING THE MPI ENVIRONMENT
############################################################
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
############################################################

############################################################
# MAKING A NUMPY FILE OF ALL THE IMAGES THAT NEED TO BE READ
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='convertSeqFiles',names=['inputFile','outputDir','outputPNGFlag','outputNPYFlag','scalingFactor','averageFrames','firstFrame','lastFrame'])

inputFileList,outputDirList,outputPNGFlagList,outputPNGDirList,outputNPYFlagList,outputNPYDirList,outputNPYDirList,scalingFactorList,firstFrameList,lastFrameList,darkRefFileList,gainRefFileList = [],[],[],[],[],[],[],[],[],[],[],[]
for inputFile,outputDir,outputPNGFlag,outputNPYFlag,scalingFactor,averageFrames,firstFrame,lastFrame in df.values:
    darkRefFile = inputFile.replace('.seq','.seq.dark.mrc')
    gainRefFile = inputFile.replace('.seq','.seq.gain.mrc')
    
    try:
        if (numpy.isnan(outputDir)):
            outputDir = inputFile.split('.seq')[0]+'_converted'
    except:
        pass
    outputPNGDir = outputDir+'/png'
    outputNPYDir = outputDir+'/npy'
    if (numpy.isnan(outputPNGFlag)):
        outputPNGFlag = 1
    if (numpy.isnan(scalingFactor)):
        scalingFactor = 1
    if (numpy.isnan(firstFrame)):
        firstFrame = 1
    if (numpy.isnan(lastFrame)):
        lastFrame = 1e10
        
    if (rank==0 and (outputPNGFlag==1 or outputNPYFlag==1)):
        utils.mkdir(outputDir)
        if (outputPNGFlag==1):
            utils.mkdir(outputPNGDir)
        if (outputNPYFlag==1):
            utils.mkdir(outputNPYDir)
            
    inputFileList.append(inputFile)
    outputDirList.append(outputDir)
    outputPNGFlagList.append(outputPNGFlag)
    outputPNGDirList.append(outputPNGDir)
    outputNPYFlagList.append(outputNPYFlag)
    outputNPYDirList.append(outputNPYDir)
    scalingFactorList.append(scalingFactor)
    firstFrameList.append(firstFrame)
    lastFrameList.append(lastFrame)
    darkRefFileList.append(darkRefFile)
    gainRefFileList.append(gainRefFile)
comm.barrier()
############################################################

############################################################
# READING THE INPUT FILES AND STORING AS PNG SEQUENCE
############################################################
for inputFile,outputDir,outputPNGFlag,outputPNGDir,outputNPYFlag,outputNPYDir,scalingFactor,firstFrame,lastFrame,darkRefFile,gainRefFile in zip(inputFileList,outputDirList,outputPNGFlagList,outputPNGDirList,outputNPYFlagList,outputNPYDirList,scalingFactorList,firstFrameList,lastFrameList,darkRefFileList,gainRefFileList):
    if (rank==0):
        print ("Parsing %s" %(inputFile))
        
    ############################################################
    # READING THE INPUT FILE AND GETTING SOME KEY PARAMETERS
    images = pims.open(inputFile)
    [row,col,numFrames] = images.header_dict['height'],images.header_dict['width'],images.header_dict['allocated_frames']
    if (numFrames<lastFrame):
        frameList = range(int(firstFrame),numFrames+1)
    else:
        frameList = range(int(firstFrame),int(lastFrame)+1)
    procFrameList = numpy.array_split(frameList,size)
    
    ############################################################
    # SETTING UP THE DARK AND GAIN REFERENCEING
    [darkRef,_] = mrcz.readMRC(darkRefFile)
    [gainRef,_] = mrcz.readMRC(gainRefFile)
    if (gainRef.mean()<=10):
        correction = gainRef.copy()
    else:
        correction = gainRef-darkRef
    correction[numpy.abs(correction)<1e-5] = 1
    median = numpy.median(correction)
    
    ############################################################
    # FIRST PASS THROUGH ALL FILES TO GET THE CONTRAST VALUE
    outFile = open('convertSeqFileLog_'+str(rank)+'.dat','w')
    outFile.write("InputFile\tFrame\tCount\tPixInAngstrom (TODO)\tDoseRate\tYear\tMonth\tDay\tHour\tMinute\tSecond\tMicrosecond\n")
    contrastLimit = open(str(rank)+'.dat','w')
    minCount,maxCount=1e10,-1e10
    comm.Barrier()
    for frame in tqdm(procFrameList[rank]):
        gImg = images.get_frame(frame-1)
        dateTime,timeAbs = images.get_time(frame-1),images.get_time_float(frame-1)
        gImg = (gImg-darkRef)/correction*median
        electronCount = numpy.mean(gImg)
        pixInAngstrom,doseRate = electronCount,electronCount
        if (gImg.min()<minCount):
            minCount = gImg.min()
        if (gImg.max()>maxCount):
            maxCount = gImg.max()
        outFile.write("%s\t%d\t%.6f\t%.6f\t%.6f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" %(inputFile,frame,electronCount,pixInAngstrom,doseRate,dateTime.year,dateTime.month,dateTime.day,dateTime.hour,dateTime.minute,dateTime.second,dateTime.microsecond))
    contrastLimit.write("%f\t%f" %(minCount,maxCount))
    contrastLimit.close()
    outFile.close()
    comm.barrier()
    
    ############################################################
    # COMPILE INFORMATION FROM LOG FILES AND CONTRAST FILES
    if (rank==0):
        contrastLimit = open('contrastLimit.dat','w')
        for r in range(size):
            inFile = open(str(r)+'.dat','r')
            for line in inFile:
                contrastLimit.write(line+'\n')
            inFile.close()
            os.remove(str(r)+'.dat')
        contrastLimit.close()
    comm.Barrier()
    
    if (rank==0):
        for r in range(size):
            if (r==0):
                df = pandas.read_csv('convertSeqFileLog_'+str(r)+'.dat',delimiter='\t',header=0)
            else:
                df = df.append(pandas.read_csv('convertSeqFileLog_'+str(r)+'.dat',delimiter='\t',header=0),ignore_index=True)
            os.remove('convertSeqFileLog_'+str(r)+'.dat')
        df.to_csv('convertSeqFileLog_'+inputFile.split('/')[-1]+'.dat',sep='\t',index=False,header=True,na_rep='nan')
        del df
    gc.collect()
    comm.barrier()
    
    ############################################################
    # SECOND PASS THROUGH ALL FILES TO SAVE IMAGES WITH APPROPIATE CONTRAST
    if (outputPNGFlag==1 or outputNPYFlag==1):
        contrastLimit = numpy.loadtxt('contrastLimit.dat')
        try:
            minCount,maxCount = contrastLimit[:,0].min(),contrastLimit[:,1].max()
        except:
            minCount,maxCount = contrastLimit.min(),contrastLimit.max()
        for frame in tqdm(procFrameList[rank]):
            if (frame>=firstFrame and frame<=lastFrame):
                gImg = images[frame-1]
                gImg = (gImg-darkRef)/correction*median
                if (outputNPYFlag==1):
                    outputNPYFile = outputNPYDir+'/'+str(frame).zfill(6)+'.npy'
                    numpy.save(outputNPYFile,gImg)
                if (outputPNGFlag==1):
                    if (float(scalingFactor)<1):
                        gImg = cv2.resize(gImg,(int(col*float(scalingFactor)),int(row*float(scalingFactor))),interpolation=cv2.INTER_AREA)
                    elif (float(scalingFactor)>1):
                        gImg = cv2.resize(gImg,(int(col*float(scalingFactor)),int(row*float(scalingFactor))),interpolation=cv2.INTER_LINEAR)
                    gImg = utils.normalize(gImg,min=(gImg.min()-minCount)/(maxCount-minCount)*255,max=(gImg.max()-minCount)/(maxCount-minCount)*255)
                    outputPNGFile = outputPNGDir+'/'+str(frame).zfill(6)+'.png'
                    cv2.imwrite(outputPNGFile,gImg)
    comm.barrier()
    
    if (rank==0):
        os.remove('contrastLimit.dat')
    images.close()
    comm.barrier()
############################################################

############################################################
# COMBINE THE LOG FILES TOGETHER
############################################################
if (rank==0):
    for inputFile in inputFileList:
        try:
            if (inputFile==inputFileList[0]):
                df = pandas.read_csv('convertSeqFileLog_'+inputFile.split('/')[-1]+'.dat',delimiter='\t',header=0)
            else:
                df = df.append(pandas.read_csv('convertSeqFileLog_'+inputFile.split('/')[-1]+'.dat',delimiter='\t',header=0),ignore_index=True)
            os.remove('convertSeqFileLog_'+inputFile.split('/')[-1]+'.dat')
        except:
            print(inputFile, "does not exist")
    df.to_csv('convertSeqFileLog.dat',sep='\t',index=False,header=True,na_rep='nan')
    del df
    gc.collect()
comm.barrier()
############################################################
