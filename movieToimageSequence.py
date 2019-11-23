import numpy
import imageio
import pandas
import utils
from tqdm import tqdm

df = pandas.read_excel('inputs.xlsx',sheet_name='movieToimageSequence',names=['inputFile','outputDir','grayscaleFlag'])

for inputFile,outputDir,grayscaleFlag in df.values:
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
    if (numpy.isnan(grayscaleFlag)):
        grayscaleFlag = 0
        
    try:
        reader = imageio.get_reader(inputFile)
        col,row = reader.get_meta_data()['size']
        numFrames = reader.count_frames()
        utils.mkdir(outputDir)
        for frame in tqdm(range(numFrames)):
            img = reader.get_data(frame)
            if (grayscaleFlag==1):
                img = utils.RGBtoGray(img)
            imageio.imwrite(outputDir+'/'+str(frame+1).zfill(6)+'.png',img)
        reader.close()
    except:
        print('Cannot not convert %s. File corrupt or format not supported.' %(inputFile))
