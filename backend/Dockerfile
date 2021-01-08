FROM python:3.7.3-slim
RUN pip install --upgrade pip
COPY requirements.txt /
RUN pip3 install -r /requirements.txt
COPY . /app
WORKDIR /app
EXPOSE 5000
ENTRYPOINT [ "gunicorn", "-w", "4", "app:app", "-b", ":5000"]

# docker build -t flask-api-starter .
# 
# For Development Container
# docker run -dt --name=flask-api-starter -v $PWD:/app -p 5000:5000 -e 'WORK_ENV=DEV' flask-api-starter
# 
# For Production Container
# docker run -dt --restart=always --name=flask-api-starter -p 5000:5000 -e 'WORK_ENV=PROD' flask-api-starter
# 
# Remove the container
# docker rm -f flask-api-starter

# docker logs --follow flask-api-starter
# docker exec -it flask-api-starter bash
