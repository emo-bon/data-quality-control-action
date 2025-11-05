FROM python:3.9
RUN pip install beautifulsoup4 pandas pygithub pyyaml uritemplate
RUN pip install git+https://github.com/vliz-be-opsci/py-data-rules.git@main
COPY action /opt/action
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh
ENTRYPOINT ["/opt/entrypoint.sh"]
