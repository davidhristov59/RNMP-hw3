FROM apache/spark:3.4.3

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip && \
    pip3 install pyspark==3.4.3 notebook==6.5.4 pandas numpy scikit-learn

RUN mkdir -p /workspace/notebooks /workspace/data

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]