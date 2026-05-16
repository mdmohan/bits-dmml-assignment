FROM postgres:16

WORKDIR /bootstrap
COPY bootstrap /bootstrap
COPY bootstrap_entrypoint.sh /bootstrap/bootstrap_entrypoint.sh
RUN chmod +x /bootstrap/bootstrap_entrypoint.sh /bootstrap/run_bootstrap.sh

ENTRYPOINT ["/bootstrap/bootstrap_entrypoint.sh"]
