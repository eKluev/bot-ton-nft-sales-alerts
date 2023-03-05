FROM python:3.10
ENV TZ="Europe/Moscow"
WORKDIR /usr/src/app/
COPY . /usr/src/app/
RUN pip install --no-cache-dir --upgrade -r requirements.txt
EXPOSE 80
CMD [ "python", "./app.py" ]