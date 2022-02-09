FROM python:3.9

ENV CLOUDSDK_INSTALL_DIR /usr/local/gcloud
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

COPY . .

RUN pip install -r requirements.txt

CMD exec /bin/sh -c "trap : TERM INT; sleep 1d & wait"
