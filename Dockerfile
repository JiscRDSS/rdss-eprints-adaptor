FROM python:3.6

RUN apt-get update && apt-get -y install cron
COPY crontab /etc/cron.d/run-eprints-adaptor-cron
RUN chmod 0644 /etc/cron.d/run-eprints-adaptor-cron

WORKDIR /app
COPY run.py /app
COPY app/ /app

RUN pip install -r requirements.txt

RUN python ./certificates/certifi_append.py ./certificates/QuoVadis_Global_SSL_ICA_G2.pem

CMD printenv >> /etc/environment && cron -f
