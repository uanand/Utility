import pandas
import subprocess
import platform

############################################################
# READ THE INPUT EXCEL FILE AND ZIP ARCHIVE THE FOLDERS
############################################################
df = pandas.read_excel('inputs.xlsx',sheet_name='archiveFolder',names=['inputDir','deleteFlag'])

if (platform.system()=='Linux'):
    if ('LinuxMint' in platform.platform()):
        executable = '7z'
    elif ('centos' in platform.platform()):
        executable = 'zip'
elif (platform.system()=='Windows'):
    executable = 'C:/Program Files/7-Zip/7z.exe'
for inputDir,deleteFlag in df.values:
    print ('Archiving the folder %s' %(inputDir))
    zipFileName = inputDir+'.zip'
    command = executable + zipFileName + ' ' + inputDir
    if (deleteFlag==1):
        if (executable=='zip'):
            subprocess.run([executable,'-qrm9T',zipFileName,inputDir])
        else:
            subprocess.run([executable,'a',zipFileName,inputDir,'-sdel'])
    else:
        if (executable=='zip'):
            subprocess.run([executable,'-qr9T',zipFileName,inputDir])
        else:
            subprocess.run([executable,'a',zipFileName,inputDir])
############################################################
