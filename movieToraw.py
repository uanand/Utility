import os
import numpy
import pandas
import utils
from tqdm import tqdm

df = pandas.read_excel('inputs.xlsx',sheet_name='movieToraw',names=['inputFile','outputDir'])

for inputFile,outputDir in df.values:
    print ('Converting movie %s to image sequence' %(inputFile))
    try:
        if (numpy.isnan(outputDir)):
            if ('.mp4' in inputFile):
                outputDir = inputFile.replace('.mp4','')
            elif ('.avi' in inputFile):
                outputDir = inputFile.replace('.avi','')
            elif ('.mov' in inputFile):
                outputDir = inputFile.replace('.mov','')
            elif ('.mkv' in inputFile):
                outputDir = inputFile.replace('.mkv','')
            elif ('.mpg' in inputFile):
                outputDir = inputFile.replace('.mpg','')
            elif ('.mpeg' in inputFile):
                outputDir = inputFile.replace('.mpeg','')
            elif ('.wmv' in inputFile):
                outputDir = inputFile.replace('.wmv','')
    except:
        pass
        
    try:
        mkvMovie = inputFile
        inputDir = mkvMovie.split("/spin_server.mkv")[0]
        mkvMovie = "spin_server.mkv" #inputFile
        rawMovie = outputDir + ".raw"
        os.chdir(inputDir)
        print (mkvMovie, outputDir)
        os.system("gst-launch-1.0 filesrc location=%s ! matroskademux name=demux ! queue ! h265parse ! avdec_h265 ! videoconvert ! video/x-raw, format=GRAY16_LE ! filesink location=%s" %(mkvMovie,rawMovie))
        # gst-launch-1.0 filesrc location=spin_cam_full.mkv ! matroskademux name=demux ! queue ! h265parse ! avdec_h265 ! videoconvert ! video/x-raw, format=GRAY16_LE ! filesink location=ingame.mkv_0.raw
    except:
        print('Cannot not convert %s. File corrupt or format not supported.' %(inputFile))
