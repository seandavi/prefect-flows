FROM python:3.9

COPY . .

RUN pip install -r requirements.txt

CMD exec /bin/sh -c "trap : TERM INT; sleep 1d & wait"
