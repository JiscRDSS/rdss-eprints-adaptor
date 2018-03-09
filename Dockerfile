FROM python:3.6

RUN apt-get update && apt-get -y install cron
COPY crontab /etc/cron.d/run-eprints-adaptor-cron
RUN chmod 0644 /etc/cron.d/run-eprints-adaptor-cron
RUN sed -i 's/EPRINTS_URL/$EPRINTS_URL/' /etc/cron.d/run-eprints-adaptor-cron
RUN sed -i 's/DYNAMODB_WATERMARK_TABLE_NAME/$DYNAMODB_WATERMARK_TABLE_NAME/' /etc/cron.d/run-eprints-adaptor-cron
RUN sed -i 's/DYNAMODB_PROCESSED_TABLE_NAME/$DYNAMODB_PROCESSED_TABLE_NAME/' /etc/cron.d/run-eprints-adaptor-cron
RUN sed -i 's/S3_BUCKET_NAME/$S3_BUCKET_NAME/' /etc/cron.d/run-eprints-adaptor-cron
RUN sed -i 's/OUTPUT_KINSIS_STREAM_NAME/$OUTPUT_KINSIS_STREAM_NAME/' /etc/cron.d/run-eprints-adaptor-cron

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

RUN python ./certificates/certifi_append.py ./certificates/QuoVadis_Global_SSL_ICA_G2.pem

CMD printenv >> /etc/environment && cron -f
