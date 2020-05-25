import psutil
import time
import matplotlib.pyplot as plt
from numpy import floor
from datetime import datetime,timedelta

dailyReportFrequency = 4 # AVAILABLE OPTION - 1, 2, 3, 4, 6, 8, 12, 24
refreshRate = 300 # in seconds

if (24%dailyReportFrequency!=0):
    print ('INVALID FREQUENCY. CHOOSE ONE OF THESE - 1,2,3,4,6,8,12,24')
else:
    diskRead_MBps_list,diskWrite_MBps_list,byteSent_MBps_list,byteRecv_MBps_list,time_list = [],[],[],[],[]
    
    diskRead_0 = psutil.disk_io_counters(perdisk=True)['PhysicalDrive2'].read_bytes
    diskWrite_0 = psutil.disk_io_counters(perdisk=True)['PhysicalDrive2'].write_bytes
        byteSent_0 = psutil.net_io_counters(pernic=True)['Ethernet'].bytes_sent
    byteRecv_0 = psutil.net_io_counters(pernic=True)['Ethernet'].bytes_recv
    time_0 = datetime.now()
    divisionNum0 = floor(time_0.hour/24*dailyReportFrequency)
    
    while(1):
        time.sleep(refreshRate)
        
        diskRead_1 = psutil.disk_io_counters(perdisk=True)['PhysicalDrive2'].read_bytes
        diskWrite_1 = psutil.disk_io_counters(perdisk=True)['PhysicalDrive2'].write_bytes
        byteSent_1 = psutil.net_io_counters(pernic=True)['Ethernet'].bytes_sent
        byteRecv_1 = psutil.net_io_counters(pernic=True)['Ethernet'].bytes_recv
        time_1 = datetime.now()
        
        diskRead_MBps_list.append((diskRead_1-diskRead_0)/(1024*1024)/refreshRate)
        diskWrite_MBps_list.append((diskWrite_1-diskWrite_0)/(1024*1024)/refreshRate)
        byteSent_MBps_list.append((byteSent_1-byteSent_0)/(1024*1024)/refreshRate)
        byteRecv_MBps_list.append((byteRecv_1-byteRecv_0)/(1024*1024)/refreshRate)
        time_list.append(time_1)
        
        diskRead_0 = diskRead_1
        diskWrite_0 = diskWrite_1
        byteSent_0 = byteSent_1
        byteRecv_0 = byteRecv_1
        
        divisionNum1 = floor(time_1.hour/24*dailyReportFrequency)
        
        if (divisionNum0 != divisionNum1):
            fileName = time_0.strftime("%Y%m%d %H%M%S")+'.png'
            plt.figure(figsize=(15,12))
            plt.subplot(221), plt.plot(time_list,diskRead_MBps_list,label='Disk read (MBps)'), plt.legend(), plt.xticks(rotation=45)
            plt.subplot(222), plt.plot(time_list,diskWrite_MBps_list,label='Disk write (MBps)'), plt.legend(), plt.xticks(rotation=45)
            plt.subplot(223), plt.plot(time_list,byteSent_MBps_list,label='Data sent (MBps)'), plt.legend(), plt.xticks(rotation=45)
            plt.subplot(224), plt.plot(time_list,byteRecv_MBps_list,label='Data received (MBps)'), plt.legend(), plt.xticks(rotation=45)
            plt.savefig('resourcePlots/'+fileName,format='png')
            plt.close()
            
            diskRead_MBps_list,diskWrite_MBps_list,byteSent_MBps_list,byteRecv_MBps_list,time_list = [],[],[],[],[]
            time_0 = time_1
            divisionNum0 = divisionNum1
