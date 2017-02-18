#-*-coding:utf-8-*-
#2017-2-11
#本脚本用来读取/home/luyeok/finance/hscei/和/home/luyeok/finance/csindex中的相应文件
#并将excel文件和csv文件中的数据读取出来，写入mysql数据库中
#读取、写入成功的文件移动到record_csindex和record_hscei文件夹中
#空白csv、excel文件，与数据库中相应数据重复的（每个指数每天只有一个pe值）
#则将文件移动至record_fail文件夹中，进行手动确认处理。
#同时，本脚本使用tushare抓取相应的股票、基金数据，存入数据库当中。
'''#######################################################'''
#导入pandas、MySQLdb以及slqalchemy、os、shutil库
import pandas as pd
from datetime import date,datetime
import MySQLdb
from sqlalchemy import create_engine
import os
import shutil
import tushare as ts
import time
#import pdb
#设置全局变量
#DB_ENGINE为创建的engine，为连接数据库做准备；
DB_ENGINE = create_engine(r'mysql://User:Password@localhost:3306/finance?charset=utf8')
HSCEI_PATH="/home/luyeok/finance/hscei/"
CSINDEX_PATH="/home/luyeok/finance/csindex/"
MOVE_RECORDED_EXCEL_TO="/home/luyeok/finance/record_csindex/"
MOVE_RECORDED_CSV_TO="/home/luyeok/finance/record_hscei/"
MOVE_FAILED_FILE_TO="/home/luyeok/finance/record_fail/"
#创建INDEX_TO_STOCK的dataframe，以便将指数与相应的指数基金进行对应
INDEX_TO_STOCK={'name':['上证180','上证50','沪深300','红利指数','中证500','HSCEI'],
'code':['510180','510050','510300','510880','510500','510900']}
INDEX_TO_STOCK=pd.DataFrame(INDEX_TO_STOCK)
#网络数据读取标记
#INTERNET_READ_SIGN=0
#用READ_COUNT来控制爬取网络数据失败后的间隙长度
#以及爬取网络数据的次数
#里面每个数代表的就是间隙的秒数，数的个数代表爬取多少次
READ_COUNT=[0,10,50,5,102,8,1800,3600]
MARK_TS_GET_STOCK_BASICS=0
MARK_TS_GET_TODAY_ALL=0


#将data_to_write写入mysql 数据库
def write_to_mysql(data_to_write,file_path_name=[]):
    #连接数据库
    db=DB_ENGINE.connect()
    #写入mysql数据库
    if file_path_name:
        #如果file_path_name不为空，说明有传入读取的文件
        #也就是说明不是从网上抓取的数据，而是从excel或者csv中读取的数据
        #这个时候执行以下代码，将读取的excel和csv文件分别移动到相应文件夹中
        try:
            #因为data_to_write为pandas的dataframe对象
            #可以调用to_sql方法
            #to_sql方法只能使用sqlalchemy进行初始化
            #pdb.set_trace()
            #将数据写入测试数据库
            #data_to_write.to_sql('test',db,if_exists='append',index=False)
            data_to_write.to_sql('value',db,if_exists='append',index=False)
            #pdb.set_trace()
            #通过判断后缀名，来区分将写入成功后的文件移动到相应文件夹中
            if os.path.splitext(file_path_name)[1]==".xls" or os.path.splitext(file_path_name)[1]==".xlsx":
                shutil.move(file_path_name,MOVE_RECORDED_EXCEL_TO)
            else:
                shutil.move(file_path_name,MOVE_RECORDED_CSV_TO)
        except:
            #如果写入失败，则将失败的文件移动到相应文件夹中；
            shutil.move(file_path_name, MOVE_FAILED_FILE_TO)
            print"Error Code:001. Excel or csv file Error！\n"
        #不论是否写入成功，都将数据库连接交还给sqlalchemy，以备下次使用
        db.close()
    else:
        try:
            #如果file_path_name为空，则只需要直接写入相应数据库即可
            #将数据写入测试数据库
            #data_to_write.to_sql('test',db,if_exists='append',index=False)
            data_to_write.to_sql('value',db,if_exists='append',index=False)
            #这里需要注意，写入数据库的dataframe的列标签必须与数据库一一对应，写入的时候就是根据这个列标签来写入数据库的；
            #但是，列标签的顺序可以与数据库中的顺序不一致。
            #写入如果不成功，则不对该文件做任何处理。
        except:
            print"Error Code:002.Stock in the internet Error!\n"
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
                print"Empty csv file."
                continue
            #进行数据清理，删除多余的数据
            #读取进来第一行和第二行是标题，删除；
            data_hscei.drop(data_hscei.index[[0,1]],axis=0,inplace=True)
            #读取csv文件中的日期信息，以便存入数据库中；
            #这里的日期信息使用date函数进行重组，形成date类；
            #然后，对date类使用strftime()构建成string格式，以便后面使用
            date_tmp=data_hscei.iloc[0,0]
            date_tmp=date(int(date_tmp[0:4]),int(date_tmp[4:6]),int(date_tmp[6:8]))
            date_tmp=date_tmp.strftime("%Y-%m-%d")
            #这里，将日期写入data_hscei的dataframe中，以便后面使用；
            data_hscei.iloc[0,0]=date_tmp
            #删除多余的列；
            data_hscei.drop(data_hscei.columns[range(2,9)],axis=1,inplace=True)
            #将dataframe中的指数名改成标准格式
            data_hscei.iloc[0,1]='HSCEI'
            #将dataframe中的列名改成与mysql数据库相符的名称
            #这样才能写入数据库
            data_hscei.columns=['date','name','pe']
            #为hscei数据增加code信息
            data_hscei['code']='510900'
            #得到当天相应指数基金的数据信息
            db_tmp=ts.get_k_data('510900',start=date_tmp,end=date_tmp, retry_count=3, pause=5)
            #删除获取的指数基金信息中的date列；
            #因为接下来要连接指数基金信息和从csv获取的列，这里面都有date信息
            #所以为了避免产生两个date_x，date_y，需要将获取的date列删掉。
            db_tmp.drop(['date'],axis=1,inplace=True)
            #连接data_hscei和db_tmp两个dataframe
            data_hscei=pd.merge(data_hscei,db_tmp,on='code')
            #调用数据库写入函数，将数据写入；
            write_to_mysql(data_hscei,file_path_name)
    else:
        #如果文件夹为空，则什么也不做。
        #这里什么也不做，所以使用空语句
        print"Empty CSV folder."
        pass
        
#读取excel文件
def read_excel():
    file_name=[]
    file_name=os.listdir(CSINDEX_PATH)
    #文件夹内可能没有文件，这样file_name就为空
    #这里进行判定，看文件夹内文件是否为空
    if file_name:
        for file_path_name in file_name:
            #对文件夹内的每个文件组合起绝对路径
            file_path_name=CSINDEX_PATH+file_path_name
            #pdb.set_trace()
            #因为excel文件可能为空，所以需要判断excel文件是否为空
            if os.path.getsize(file_path_name):
                #读取excel文件；
                data_csindex=pd.read_excel(file_path_name,header=None,encoding='utf-16',sep='\t')
            else:
                #如果excel为空，则将该文件移动到相应文件夹，并进行下一个文件；
                shutil.move(file_path_name, MOVE_FAILED_FILE_TO)
                print"Empty excel file."
                continue
            #获取excel文件中的日期信息
            #并将日期转换成date类，并将date类的日期转换成字符串格式
            date_tmp=data_csindex.iloc[1,3]
            date_tmp=date(int(date_tmp[1:5]),int(date_tmp[6:8]),int(date_tmp[9:11]))
            date_tmp=date_tmp.strftime("%Y-%m-%d")
            #对data_csindex进行切片，取出相应的列；
            data_csindex=data_csindex.iloc[4:15,:]
            data_csindex=data_csindex.iloc[:,[0,8]]
            #定义列名，为写入数据库做准；
            data_csindex.columns=['name','pe']
            #将日期写入data_csindex的dataframe中，以便后面使用；
            data_csindex['date']=date_tmp
            #将名称中的所有空格都去除；
            data_csindex['name']=data_csindex['name'].map(lambda x : x.replace(" ",''))
            #为csindex数据增加code信息
            #将data_csindex中的name列下的字符改成utf-8格式存储；
            #为下一步Merge连接做准备；
            data_csindex['name']=map(lambda x:x.encode('utf-8'),data_csindex['name'])
            #将data_csindex与INDEX_TO_STOCK进行左连接；
            data_csindex=pd.merge(data_csindex,INDEX_TO_STOCK,on='name',how='left')
            #针对INDEX_TO_STOCK里code列的每个值；
            #查询相应信息，每个值返回一个dataframe对象；
            #对每一个dataframe对象调用iloc[]方法，取出对应的数据行，形成一个data_tmp的list；
            data_tmp=map(lambda x:ts.get_k_data(x,start=date_tmp,end=date_tmp, retry_count=3, pause=4).iloc[0,:],INDEX_TO_STOCK['code'])
            #将data_tmp的list形成一个dataframe;
            index_data=pd.DataFrame(data_tmp)
            #为了防止merge过程中与data_csindex中的date重复
            #这里删除index_data中的date
            index_data.drop(['date'],axis=1,inplace=True)
            #将data_csindex与index_data进行merge
            data_csindex=pd.merge(data_csindex,index_data,on='code',how='left')
            #将data_csindex写入数据库；
            write_to_mysql(data_csindex,file_path_name)
    else:
        #如果文件夹为空，则什么也不做。
        #这里什么也不做，所以使用空语句
        print"Empty Excel Folder."
        pass

def TS_GET_STOCK_BASICS():
    try:
        return ts.get_stock_basics()
    except:
        MARK_TS_GET_STOCK_BASICS=404

        
def TS_GET_TODAY_ALL():
    try:
        return ts.get_today_all()
    except:
        MARK_TS_GET_TODAY_ALL=404
        

#使用tushare抓取网上股票pe信息
def read_value():
    #此函数使用Tushare来抓取网上的pe以及当天的数据
    #抓取股票的基本面信息，主要为了从基本面信息中提取pe；
    #将网络文件读取标记置0（未成功），这样在每次启动read_value的时候
    #都能够将标记重置为“未成功”
    for sleeptime in READ_COUNT:
        time.sleep(sleeptime)
        stock_basics=pd.DataFrame(TS_GET_STOCK_BASICS())
        if MARK_TS_GET_STOCK_BASICS==404 or stock_basics.empty:
            print 'n=404, we will read STOCK BASICS again!'
            continue
        else:
            print"Read STOCK BASICS Finished."
            break
        if sleeptime==3600:
            print "Read STOCK BASICS Error!"
        else:
            pass
    
    time.sleep(28)
    
    for sleeptime in READ_COUNT:
        time.sleep(sleeptime)
        stock_value=pd.DataFrame(TS_GET_TODAY_ALL())
        if MARK_TS_GET_TODAY_ALL==404 or stock_value.empty:
            print 'n=404, we will read STOCK TODAY again!'
            continue
        else:
            print"Read STOCK TODAY Finished."
            break
        if sleeptime==3600:
            print "Read STOCK TODAY Error!"
        else:
            pass
    #这里，将index转为code列存储
    stock_basics['code']=stock_basics.index
    #将抓取的数据整理成date,name,pe的形式
    stock_basics.drop(['industry','area','outstanding','totals','totalAssets','liquidAssets','fixedAssets','reserved','reservedPerShare','esp','pb','timeToMarket','undp','perundp','rev','profit','gpr','npr','holders','bvps'],axis=1,inplace=True)
    
    #给stock_pe的dataframe增添日期；
    #这里的日期就是获取的当天服务器日期；
    stock_basics['date']=time.strftime("%Y-%m-%d")
    #去除name列数据中的空格
    stock_basics['name']=stock_basics['name'].map(lambda x : x.replace(" ",''))

    #删除掉不需要的列
    stock_value.drop(['name','turnoverratio','amount','per','pb','mktcap','nmc'],axis=1,inplace=True)
    #将两个dataframe进行连接
    stock_value=pd.merge(stock_basics,stock_value,on='code',how='left')
    #因为这里我们是每天晚上收盘后抓取的数据
    #将stock_value中的trade列名改为close，以便写入数据库
    stock_value.rename(columns={'trade':'close'},inplace=True)
    write_to_mysql(stock_value)

if __name__=='__main__':
    #为输出log信息查错
    print time.strftime("%Y-%m-%d, %H:%M:%S")
    print "======read_value Begin======="
    read_value()
    print "======read_csv Begin========"
    read_csv()
    print "======read_excel Begin========"
    read_excel()
    print "======Finished============="
    #这里因为读取read_value()的值比较多，也经常失败
    #所以我们在读取全网络数据的时候，需要增加一个循环
    #判断网络数据是否读取成功
    #这里，数据库使用完毕，需要dispose相应的资源；
    DB_ENGINE.dispose()
