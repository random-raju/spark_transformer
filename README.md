# new_transformer

```
virtualenv -p python3.8 venv
source .venv/bin/activate

touch local.env

# paste the env file I send you

git clone https://github.com/libindic/indic-trans.git
cd indic-trans
pip install -r requirements.txt
python setup.py install

Download spark from here: https://spark.apache.org/downloads.html and unzip it
Download postgres connector jar and add in jars folder of the unzipped spark folder
Set these env variables too , example

export SPARK_HOME=/home/hadoop/spark-2.1.0-bin-hadoop2.7
export PATH=$PATH:/home/hadoop/spark-2.1.0-bin-hadoop2.7/bin
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
export PATH=$SPARK_HOME/python:$PATH

to test if setup is correct:
run these commands
    pyspark
    spark-submit
    
install mongodb and compass locally for testing

python3 -m urlc.mh_gom_rd_deeds_pune.pune_deeds_regular to run the job (for testing)

```

