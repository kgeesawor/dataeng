FROM python:3.10 
RUN pip install dbt
RUN pip install dbt-postgres

WORKDIR /dbt
COPY ./app /dbt/app
ENTRYPOINT ["dbt"]