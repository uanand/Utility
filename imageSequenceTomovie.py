import numpy
import imageio
import pandas
from tqdm import tqdm

df = pandas.read_excel('inputs.xlsx',sheet_name='imageSequenceTomovie',names=['inputDir','firstFrame','lastFrame','inputFileExt','outputFile','outputFileExt','quality','allCompressionFlag'])

for inputDir,firstFrame,lastFrame,inputFileExt,outputFile,outputFileExt,quality,allCompressionFlag in df.values:
    frameList = range(firstFrame,lastFrame+1)
    try:
        if (numpy.isnan(outputFile)):
            outputFile = inputDir+outputFileExt
    except:
        pass
    if (numpy.isnan(quality)):
        quality = 5
    if (numpy.isnan(allCompressionFlag)):
        allCompressionFlag = 0
    fps = int(numpy.ceil(len(frameList)/20.))
    
    if (allCompressionFlag==1):
        qualityList = range(1,11)
        for quality in qualityList:
            print ('Saving movie for %s with quality %d' %(inputDir,quality))
            output = outputFile.replace(outputFileExt,'_compress_'+str(quality).zfill(2)+outputFileExt)
            w = imageio.get_writer(output,\
                                    format='FFMPEG',\
                                    fps=fps,\
                                    codec='libx264',\
                                    quality=quality,\
                                    pixelformat='yuv420p',\
                                    macro_block_size=16)
            for frame in tqdm(frameList):
                gImg = imageio.imread(inputDir+'/'+str(frame).zfill(6)+inputFileExt)
                w.append_data(gImg)
            w.close()
    else:
        print ('Saving movie for %s with quality %d' %(inputDir,quality))
        output = outputFile.replace(outputFileExt,'_compress_'+str(quality).zfill(2)+outputFileExt)
        w = imageio.get_writer(output,\
                                format='FFMPEG',\
                                fps=fps,\
                                codec='libx264',\
                                quality=quality,\
                                pixelformat='yuv420p',\
                                macro_block_size=16)
        for frame in tqdm(frameList):
            gImg = imageio.imread(inputDir+'/'+str(frame).zfill(6)+inputFileExt)
            w.append_data(gImg)
        w.close()
    
