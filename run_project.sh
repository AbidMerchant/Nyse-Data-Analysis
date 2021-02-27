echo "---------------------STARTING SERVICES----------------------------"
start-all.sh
sh /usr/local/softwares/spark_2.4/sbin/start-all.sh


echo "---------------------SETTING UP MYSQL DATABASE--------------------"
mysql -h localhost -u hduser -pPassword@1234 < /home/hduser/work/crio_project/db_setup.sql


echo "---------------------RUNNING SPARK LOAD SCRIPTS-------------------"
unset PYSPARK_DRIVER_PYTHON

spark-submit \
	--master spark://spark-VirtualBox:7077 \
	--name load_data_to_hdfs \
	--conf spark.driver.extraClassPath=/home/hduser/work/crio_project/jars/mysql-connector-java-5.1.23-bin.jar  \
	--jars /home/hduser/work/crio_project/jars/mysql-connector-java-5.1.23-bin.jar \
	/home/hduser/work/crio_project/python_scripts/load_data_to_hdfs.py

spark-submit \
	--master spark://spark-VirtualBox:7077 \
	--name transformation s\
	--conf spark.driver.extraClassPath=/home/hduser/work/crio_project/jars/mysql-connector-java-5.1.23-bin.jar  \
	--jars /home/hduser/work/crio_project/jars/mysql-connector-java-5.1.23-bin.jar \
	/home/hduser/work/crio_project/python_scripts/transformation.py

spark-submit \
	--master spark://spark-VirtualBox:7077 \
	--name load_to_sql \
	--conf spark.driver.extraClassPath=/home/hduser/work/crio_project/jars/mysql-connector-java-5.1.23-bin.jar  \
	--jars /home/hduser/work/crio_project/jars/mysql-connector-java-5.1.23-bin.jar \
	/home/hduser/work/crio_project/python_scripts/load_to_sql.py
