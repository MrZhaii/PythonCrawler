#！ /bin/bash

# 上传到hdfs过程的重试脚本
# 执行脚本： /data/hainiu/nginx_log_bak/shell/retry_put_hdfs.sh /data/hainiu/nginx_log_bak/shell/log_cut_config  >> /data/hainiu/nginx_log_bak/log/retry_put_hdfs.log 2>&1

echo '-----------start-----------'
echo 'step1:-----------加载配置文件log_cut_config-----------'
config_file=$*


. ${config_file}
#获取当前脚本所在位置
base_path=${DATA_BASE_PATH}/shell


echo 'step2:-----------有没有put_hdfs.sh 脚本运行，如果有，停止重试-----------'
pid=`ps -aux | grep put_hdfs.sh | grep -v grep | awk '{print $2}'`
if [ "${pid}x" != "x" ]; then
	echo "put_hdfs.sh 脚本正在运行，不进行重试"
	exit
fi


echo 'step3:-----------重试3次-----------'
for((i=1;i<=3;i++))
do
	echo "step3-1: 重试第 ${i} 次"
	# 阻塞式调用put_hdfs.sh 脚本
	${base_path}/put_hdfs.sh ${base_path}/log_cut_config.sh >> ${DATA_GENERATELOG_PATH}/put_hdfs.log 2>&1
	
	
	# 检查work目录下是否有.log.gz 文件，如果没有，说明重试成功
	log_file_arr=(`ls ${DATA_WORK_PATH} | grep .log.gz`)
	
	if [ ${#log_file_arr[*]} -eq 0 ]; then
		
		echo "step3-2:-----------retry_put_hdfs success-----------"
		exit
	fi
	
	echo 'step3-2:-----------retry_put_hdfs fail-----------'
	
	#休息3秒
	sleep 3

done


echo 'step3:-----------重试3次失败-----------'
echo "重试3次失败"
echo "错误文件列表："
log_file_arr=(`ls ${DATA_WORK_PATH} | grep .log.gz`)
for file in ${log_file_arr[*]}
do
	echo $file

done

echo '-----------end-----------'
