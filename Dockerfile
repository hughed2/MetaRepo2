FROM python:3.9

WORKDIR /app/

# Make sure HTTP proxies match host environment
ENV HTTP_PROXY=$HTTP_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
ENV NO_PROXY=$NO_PROXY

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy in the code and a config file. If a config file is not provided, this will result in an error
COPY ./src /app/src

COPY ./metarepo.conf /app/metarepo.conf

# This will run on 0.0.0.0:8000 in the docker container. When using "docker run" or "docker create" with this image,
# make sure to add a "-p xxxx:8000" flag, where xxxx is the actual desired port, even if it's still 8000.
# That will enable port forwarding and allow us to actually communicate with the container.
CMD ["uvicorn", "src.metarepo:app", "--host", "0.0.0.0", "--port", "8000"]