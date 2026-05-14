FROM spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3", \
     "/app/main.py"]
