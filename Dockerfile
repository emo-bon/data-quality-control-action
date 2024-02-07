FROM python:3.9
COPY pipeline /pipeline
COPY action.py /action.py
COPY requirements.txt /requirements.txt
RUN python -m pip install -r /requirements.txt
RUN chmod +x /action.py
ENTRYPOINT ["/action.py"]
