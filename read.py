#-*-coding:utf-8-*-
#2017-2-9
#本脚本用来读取/home/luyeok/finance/hscei/和/home/luyeok/finance/csindex中的相应文件
#并将excel文件和csv文件中的数据读取出来，写入mysql数据库中
#读取、写入成功的文件移动到record_csindex和record_hscei文件夹中
#空白csv、excel文件，与数据库中相应数据重复的（每个指数每天只有一个pe值）
#则将文件移动至record_fail文件夹中，进行手动确认处理。
'''#######################################################'''
#导入pandas、MySQLdb以及slqalchemy、os、shutil库
import pandas as pd
from datetime import date,datetime
import MySQLdb 
from sqlalchemy import create_engine
import os
import shutil
#设置全局变量
#DB_ENGINE为创建的engine，为连接数据库做准备；
#这里需要注意，在ubuntu16.1，Python2.7的环境下，使用Pymsql连接会导致失败
#这里我们使用MySQLdb进行连接
DB_ENGINE = create_engine(r'mysql://mysql_username:Mysql_password@localhost:3306/finance?charset=utf8')
HSCEI_PATH="/home/luyeok/finance/hscei/"
CSINDEX_PATH="/home/luyeok/finance/csindex/"
MOVE_RECORDED_EXCEL_TO="/home/luyeok/finance/record_csindex/"
MOVE_RECORDED_CSV_TO="/home/luyeok/finance/record_hscei/"
MOVE_FAILED_FILE_TO="/home/luyeok/finance/record_fail/"

#将data_to_write写入mysql 数据库
def write_to_mysql(data_to_write,file_path_name):
    #连接数据库
    db=DB_ENGINE.connect()
    #写入mysql数据库
    try:
        #因为data_to_write为pandas的dataframe对象
        #可以调用to_sql方法
        #to_sql方法只能使用sqlalchemy进行初始化
        data_to_write.to_sql('value',db,if_exists='append',index=False)
        #通过判断后缀名，来区分将写入成功后的文件移动到相应文件夹中
        if os.path.splitext(file_path_name)[1]==".xls" or os.path.splitext(file_path_name)[1]==".xlsx":
            shutil.move(file_path_name,MOVE_RECORDED_EXCEL_TO)
        else:
            shutil.move(file_path_name,MOVE_RECORDED_CSV_TO)
    except:
        #如果写入失败，则将失败的文件移动到相应文件夹中；
        shutil.move(file_path_name, MOVE_FAILED_FILE_TO)
    #不论是否写入成功，都将数据库连接交还给sqlalchemy，以备下次使用
    db.close()

#读取csv文件
def read_csv():
    file_name=[]
    #取得文件夹下的所有文件名称
    file_name=os.listdir(HSCEI_PATH)
    #文件夹内可能没有文件，这样file_name就为空
    #这里进行判定，看文件夹内文件是否为空
    if file_name:
        for file_path_name in file_name:
            #对文件夹内的每个文件组合起绝对路径
            file_path_name=HSCEI_PATH+file_path_name
            #因为csv文件可能为空，所以需要判断csv文件是否为空
            if os.path.getsize(file_path_name):
                #读取csv文件，并将读取的结果存入dataframe格式
                data_hscei = pd.read_csv(file_path_name,header=None, encoding='utf-16',sep='\t') 
            else:
                #如果csv为空，则将该文件移动到相应文件夹，并进行下一个文件；
                shutil.move(file_path_name, MOVE_FAILED_FILE_TO)
                continue
            #进行数据清理，删除多余的数据
            data_hscei.drop(data_hscei.index[[0,1]],axis=0,inplace=True)
            data_hscei.drop(data_hscei.columns[range(2,9)],axis=1,inplace=True)
            date_tmp=data_hscei.iloc[0,0]
            #这里是将文件中的日期转换成标准日期格式
            data_hscei.iloc[0,0]=date(int(date_tmp[0:4]),int(date_tmp[4:6]),int(date_tmp[6:8]))
            #将dataframe中的指数名改成标准格式
            data_hscei.iloc[0,1]='HSCEI'
            #将dataframe中的列名改成与mysql数据库相符的名称
            #这样才能写入数据库
            data_hscei.columns=['date','name','pe']
            #调用数据库写入函数，将数据写入；
            write_to_mysql(data_hscei,file_path_name)
    else:
        #这里什么也不做，所以使用空语句
        pass

def read_excel():
    file_name=[]
    file_name=os.listdir(CSINDEX_PATH)
    if file_name:
        for file_path_name in file_name:
            file_path_name=CSINDEX_PATH+file_path_name
            if os.path.getsize(file_path_name):
                data_csindex=pd.read_excel(file_path_name,header=None,encoding='utf-16',sep='\t')
            else:
                shutil.move(file_path_name, MOVE_FAILED_FILE_TO)
                continue
            date_tmp=data_csindex.iloc[1,3]
            date_tmp=date(int(date_tmp[1:5]),int(date_tmp[6:8]),int(date_tmp[9:11]))
            data_csindex=data_csindex.iloc[4:15,:]
            data_csindex=data_csindex.iloc[:,[0,8]]
            data_csindex.columns=['name','pe']
            data_csindex['date']=date_tmp
            #将名称中的所有空格都去除；
            data_csindex['name']=data_csindex['name'].map(lambda x : x.replace(" ",''))
            write_to_mysql(data_csindex,file_path_name)
    else:
        #这里什么也不做，所以使用空语句
        pass

if __name__=='__main__':
    read_csv()
    read_excel()
    #这里，数据库使用完毕，需要dispose相应的资源；
    DB_ENGINE.dispose()
