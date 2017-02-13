#!/bin/bash
###这一段代码用来备份finance数据库
###并按照日期进行命名


#备份的路径
backup_route=/home/luyeok/backup/mysql_backup/
#备份的日期
var_date=$(date +%Y-%m-%d)
Database_name=finance
Att=.sql
#组合文件名
backup_path_name=${backup_route}${Database_name}"_"${var_date}${Att}

###对数据库进行备份
mysqldump -hlocalhost -P3306 -uluyeok -pForrestrun88$% $Database_name > $backup_path_name

###每天对数据库进行备份打包
var_5=/home/luyeok/backup/mysql_backup/finance_
var_tar=.tar.gz
var_backup_tar=${var_5}${var_date}${var_tar}
tar -zcf ${var_backup_tar} $backup_path_name

###删除备份的sql文件
rm $backup_path_name