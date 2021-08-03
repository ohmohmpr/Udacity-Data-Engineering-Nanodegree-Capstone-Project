FROM jupyter/pyspark-notebook:spark-3.1.2

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN rm -rf requirements.txt