#!/usr/bin/env bash
 
http_proxy="http://proxy.ouraes.com:8080"
export http_proxy
https_proxy="https://proxy.ouraes.com:8080"
export https_proxy


sudo apt update

sudo apt install python3-pip

pip install pyspark
pip install pandas
pip install RegscorePy
pip install scikit-plot
pip install imblearn
pip install SQLAlchemy
pip install alembic
pip install mlflow
pip install bs4
pip install xgboost
pip install holidays
pip install lxml
pip install google-cloud-storage
pip install google-cloud-bigquery==1.26.0
pip install dateparser
pip install tabula-py
pip install more-itertools
pip install pandas-gbq==0.12
pip install factor_analyzer
pip install xlrd
pip install gcsfs