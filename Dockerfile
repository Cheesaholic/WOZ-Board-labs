FROM python:3.7-stretch

RUN apk add --no-cache git
ADD requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

ADD . /opt/woz-board
WORKDIR /opt/woz-board

CMD [ "python", "bokeh serve --show /opt/woz-board" ]