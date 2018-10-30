FROM python:3.6

RUN apt-get update && apt-get -y install cron
COPY crontab /etc/cron.d/run-eprints-adaptor-cron
RUN chmod 0644 /etc/cron.d/run-eprints-adaptor-cron

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

CMD printenv >> /etc/environment && cron -f
