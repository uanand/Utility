import numpy
import cv2
import pandas
from skimage.feature import match_template

df = pandas.read_excel('inputs.xlsx',sheet_name='findCropInImage',names=['cropImg','originalImg'])

for cropImgFile,gImgFile in df.values:
    gImg = cv2.imread(gImgFile,0)
    cropImg = cv2.imread(cropImgFile,0)
    rowCrop,colCrop = cropImg.shape
    result = match_template(gImg,cropImg)
    ij = numpy.unravel_index(numpy.argmax(result),result.shape)
    x,y = ij[::-1]
    print ('%s\tStartrow:%d\tStartcol:%d\tHeight:%d\tWidth:%d\tmakeRectangle(%d, %d, %d, %d)' %(cropImgFile,y,x,rowCrop,colCrop,x,y,colCrop,rowCrop))
