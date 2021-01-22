FROM python:3.7.3-slim
ENV TEST_ENV 'value'
RUN pip install --upgrade pip
COPY requirements.txt /
RUN pip3 install -r /requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org
COPY . /app
WORKDIR /app
EXPOSE 5000
ENTRYPOINT [ "gunicorn", "-w", "4", "app:app", "-b", ":5000"]