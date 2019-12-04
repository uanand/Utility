import os
import numpy
from os.path import isdir,join
from os import listdir

############################################################
# CREATE DIRECTORY
############################################################
def mkdir(dirName):
    if (os.path.exists(dirName) == False):
        os.makedirs(dirName)
############################################################

############################################################
# CONVERT RGB IMAGE TO GRAYSCALE
############################################################
def RGBtoGray(img):
    gImg = (img[:,:,0]*0.30 + img[:,:,1]*0.59 + img[:,:,2]*0.11).astype('uint8')
    return gImg
############################################################

############################################################
# IMAGE NORMALIZING FUNCTION
############################################################
def normalize(gImg, min=0, max=255):
    if (gImg.max() > gImg.min()):
        gImg = 1.0*(max-min)*(gImg - gImg.min())/(gImg.max() - gImg.min())
        gImg=gImg+min
    elif (gImg.max() > 0):
        gImg[:] = max
    gImg=gImg.astype('uint8')
    return gImg
############################################################

####################################################
# GET LIST OF DM4 FILES IN DIRECTORY
####################################################
def getFiles(path,extension='dm4'):
    k=0
    inputFileList=[]
    for root,dirs,files in os.walk(path):
        for name in files:
            if name.endswith((extension)):
                inputFileList.append(join(root,name))
                k=k+1
    inputFileList = numpy.sort(inputFileList,kind='mergesort')
    return inputFileList
####################################################
