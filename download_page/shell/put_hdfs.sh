#! /bin/bash

# 把work 目录下的.log.gz 格式的文件上传到hdfs目录上
# 执行脚本： /data/hainiu/nginx_log_bak/shell/put_hdfs.sh /data/hainiu/nginx_log_bak/shell/log_cut_config.sh > /data/hainiu/nginx_log_bak/log/put_hdfs.log 2>&1

echo '-----------start-----------'
echo 'step1:-----------加载配置文件log_cut_config-----------'
config_file=$*

. ${config_file}

#获取当前脚本所在位置
base_path=${DATA_BASE_PATH}/shell


echo 'step2:-----------校验配置文件的参数-----------'
#无效参数
invalid=false
if [ "${DATA_PATH}x" == "x" ]; then
	invalid=true
fi

if [ "${DATA_BASE_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_WORK_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_BAK_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_GENERATELOG_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_HDFS_BASE_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${LOG_USER}x" == "x" ]; then
	invalid=true
fi

if [ "${invalid}" == "true" ]; then
	echo "log_cut_config.sh params invalid"
	exit
fi 

echo 'step3:-----------上传hdfs-----------'
# 上work目录下，找.log.gz 文件
log_file_arr=(`ls ${DATA_WORK_PATH} | grep .log.gz`)

#统计上传失败的文件数
put_hdfs_error_count=0

put_hdfs_path=${DATA_HDFS_BASE_PATH}

for file in ${log_file_arr[*]}
do
	#file: access_192.168.142.160_20190420112437.log.gz
	
	# 确定上传的hdfs目录
	# access_192.168.142.160_20190420112233.log.gz  ---> month=04 day=20
	# /data/hainiu/nginx_log/${month}/${day}/xxxx
	# tmp:20190420112437.log.gz
	tmp=${file##*_}
	month=${tmp:0:6}
	day=${tmp:6:2}

	echo 'step3-1:-----------创建上传hdfs目录-----------'
	put_hdfs_path=${DATA_HDFS_BASE_PATH}/${month}/${day}
	# 创建hdfs目录 /data/hainiu/nginx_log/04/20
	mkdir_result=`/usr/local/hadoop/bin/hadoop fs -mkdir -p ${put_hdfs_path} 2>&1`
	
	if [ "${mkdir_result}x" != "x" ]; then
		if [ "`echo ${mkdir_result##*:}`" != "File exists" ]; then
			echo "hadoop fs -mkdir -p ${put_hdfs_path} error"
			echo "detail info: ${mkdir_result}"
			exit
		fi
	fi
	
	echo 'step3-2:-----------上传文件到hdfs上-----------'
	# 上传操作
	put_result=`/usr/local/hadoop/bin/hadoop fs -put -f ${DATA_WORK_PATH}/${file} ${put_hdfs_path} 2>&1`
	# 上传报错
	if [ "${put_result}x" != "x" ]; then
		echo "hadoop fs -put -f ${DATA_WORK_PATH}/${file} ${put_hdfs_path} error"
		echo "detail info: ${put_result}"
		# 错误次数+1
		 echo 'step3-3:-----------上传文件到hdfs失败-----------'
		((put_hdfs_error_count++))
	else
		echo 'step3-3:-----------上传文件到hdfs成功-----------'
		# 上传成功，删除work目录下的文件
		rm -f ${DATA_WORK_PATH}/${file}
	fi		
done

# 如果目录下没有文件，就直接退出
if [ ${#log_file_arr[*]} -eq 0 ]; then
	exit
fi



if [ $put_hdfs_error_count -eq 0 ]; then
	echo 'step4-1:-----------上传都成功，创建hdfs目录上创建一个已标记文件-----------'
	#创建成功标记文件
	timestmap=`date +%Y%m%d%H%M%S`
	success_file=_SUCCESS_${timestmap}
	touch_result=`/usr/local/hadoop/bin/hadoop fs -touchz ${put_hdfs_path}/${success_file} 2>&1`
	if [ "${touch_result}x" != "x" ]; then
		echo "hadoop fs -put -f -touchz ${put_hdfs_path}/${success_file} error"
		echo "detail info: ${touch_result}"
	fi

else 
	echo 'step4-1:-----------调用重试脚本进行重试-----------'
	# 有没有put_retry_hdfs.sh 脚本运行，如果有，停止重试
	pid=`ps -aux | grep put_retry_hdfs.sh | grep -v grep | awk '{print $2}'`
	if [ "${pid}x" != "x" ]; then
		echo "put_retry_hdfs.sh 脚本正在运行，不进行重试"
		exit
	fi
	
	nohup ${base_path}/put_retry_hdfs.sh ${base_path}/log_cut_config.sh >> ${DATA_GENERATELOG_PATH}/put_retry_hdfs.log 2>&1 &

fi


echo 'step5:-----------end-----------'
