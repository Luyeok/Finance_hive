#!/bin/bash
#本段代码用于实现发送每日报告；
var_date=$(date +%Y-%m-%d)
var_read_script=股票信息读取脚本

#发送read.py.log的后14行至邮箱luyeok@gmail.com
tail -n 14 read.py.log 2>&1 | mutt -s ${var_date}"-"${var_read_script} luyeok@gmail.com
