

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
source ~/miniconda3/bin/activate
conda update conda

conda create --name test_env python=3.8
conda activate test_env
conda install pandas
conda install matplotlib
conda install seaborn
conda install jupyter 
conda install ipykernel 
conda install conda-forge::python-confluent-kafka
conda install pyspark 
conda install findspark 

wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar

