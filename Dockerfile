FROM python:3.9
COPY entrypoint.sh /opt/entrypoint.sh
COPY action /opt/action
COPY requirements.txt /opt/requirements.txt
RUN chmod +x /opt/entrypoint.sh
RUN pip install -r /opt/requirements.txt
ENTRYPOINT ["/opt/entrypoint.sh"]
