#!/bin/bash
###这一段代码用来将csindex的数据下载到本地~/finance/csindex文件夹中
###并按照日期进行命名
#组合文件名
var_1=/home/luyeok/finance/csindex/csindex_
var_date=$(date +%Y-%m-%d)
var_append=.xls
var_csindex_path=${var_1}${var_date}${var_append}
#下载中证相关数据，尝试20次，断电续传，并将相关文件存储至finance/csindex文件夹下test.xls文件中
wget --user-agent="Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.204 Safari/534.16" --tries=5 --wait=180 -c -O ${var_csindex_path} ftp://115.29.204.48/webdata/indexinfo.xls

###这一段代码用来将hscei的数据下载到本地~/finance/hscei文件夹中
###并按照日期进行命名
#组合存储文件名
var_2=/home/luyeok/finance/hscei/hscei_
var_append_csv=.csv
var_hscei_path=${var_2}${var_date}${var_append_csv}
#组合下载文件名
var_3=http://sc.hangseng.com/gb/www.hsi.com.hk/HSI-Net/static/revamp/contents/en/indexes/report/hscei/idx_
var_4=$(date +%-d%b%y)
var_hscei_dl_path=${var_3}${var_4}${var_append_csv}
wget --user-agent="Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.204 Safari/534.16" --tries=5 --wait=180 -c -O ${var_hscei_path} ${var_hscei_dl_path}

###每天对数据库进行备份打包
var_5=/home/luyeok/backup/finance_
var_tar=.tar.gz
var_backup=${var_5}${var_date}${var_tar}
tar -zcf ${var_backup} ~/finance
