FROM python:3.8.11-slim-buster
RUN mkdir /app
WORKDIR /app
RUN pip install tornado graphene-tornado
COPY . /app/
CMD ["python", "server.py"]