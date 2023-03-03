FROM nannyml/nannyml:0.8.2

COPY ["app.py", "estimator.py", "cloud_watch_client.py", "requirements.txt", "./"]
RUN pip3 install -r requirements.txt --target .

ENTRYPOINT [ "/usr/local/bin/python3", "-m", "awslambdaric" ]
CMD [ "app.handler" ]