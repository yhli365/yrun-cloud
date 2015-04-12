ssh hadoop@192.168.1.218
#运行beeline
$SPARK_HOME/bin/beeline
beeline> !connect jdbc:hive2://devs1:10000 scott tiger org.apache.hive.jdbc.HiveDriver
0: jdbc:hive2://devs1:10000> 
show tables;

#退出
0: jdbc:hive2://devs1:10000> !quit

-----------------采集输入条件
--手机号
select MSISDN,count(1) cnt from im group by MSISDN order by cnt desc limit 30;

------------------#随机读
--手机号
select pname,protocol_id,data_id,capture_time,bsid,MSISDN,user_name,from_user_name,to_user_name from im where MSISDN='19377579818' limit 10;



