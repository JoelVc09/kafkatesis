

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
