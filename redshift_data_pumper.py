import json
import sqlalchemy as sa
import pandas as pd


class RedshiftDataPumper:

    def __init__(self,
                 iam_role,
                 source_region,
                 target_region):

        self.iam_role = iam_role
        self.source_region = source_region
        self.target_region = target_region

    def get_redshift_connection(self, creds_path):

        """
         Create a sqlalchemy connection to the redshift backup db
         :param creds_path: a path to a json file with values for a connection string
         :return: a sqlalchemy engine
         """

        with open(creds_path) as f:
            rs_creds = json.load(f)

        engine_connect_string = 'redshift+psycopg2://{db_user}:{db_pw}@{db_host}:{port}/{db_name}'.format(
            db_user=rs_creds["username"],
            db_pw=rs_creds["pw"],
            db_host=rs_creds["host"],
            port=rs_creds["port"],
            db_name=rs_creds["db_name"]
        )

        return sa.create_engine(engine_connect_string)

    def unload_to_s3(self, bucket, schema, table, creds_path, delimiter=None):

        """
        Unloads a table from redshift into s3.

        :param bucket: a full bucket path, including the target file
        :param schema: a string of the schema name to be queried from
        :param table: a string of the table name to ve queried from
        :param creds_path: a path to a json file with the connection string values of the target table
        :param delimiter: a string of the preferred delimiter. If none, defaults to pipe.
        :return: nothing
        """

        if delimiter is None:
            delimiter = '|'

        unload_template = """
        unload ('select * from {}.{}')
        to 's3://{}'
        iam_role 'arn:aws:iam::{}:role/redshift-s3'
        parallel off
        delimiter '{}' addquotes allowoverwrite escape;"""

        engine = self.get_redshift_connection(creds_path).connect()

        engine.execute(unload_template.format(schema, table, bucket, self.iam_role, delimiter))

    def unload_schema_to_s3(self, bucket, schema, creds_path, delimiter=None):

        if delimiter is None:
            delimiter = '|'

        engine = self.get_redshift_connection(creds_path).connect()
        table_query = "select table_name from information_schema.tables where table_schema = '{}'"
        tables = pd.read_sql_query(table_query.format(schema), engine)

        tables_list = list(tables['table_name'])

        unload_template = """
                unload ('select * from {}.{}')
                to 's3://{}/{}'
                iam_role 'arn:aws:iam::{}:role/redshift-s3'
                parallel off
                delimiter '{}' addquotes allowoverwrite escape;"""

        for t in tables_list:
            engine.execute(unload_template.format(schema, t, bucket, t, self.iam_role, delimiter))

    def copy_schema_from_s3(self, bucket, schema, creds_path, delimiter=None):

        if delimiter is None:
            delimiter = '|'

        engine = self.get_redshift_connection(creds_path).connect()
        table_query = "select table_name from information_schema.tables where table_schema = '{}'"
        tables = pd.read_sql_query(table_query.format(schema), engine)

        tables_list = list(tables['table_name'])

        copy_template = """COPY {}.{} FROM 's3://{}/{}000'
        credentials 'aws_iam_role=arn:aws:iam::{}:role/RedshiftCopyUpload'
        region '{}'
        timeformat 'auto'
        delimiter '{}' REMOVEQUOTES ACCEPTANYDATE ESCAPE"""

        for t in tables_list:
            engine.execute(copy_template.format(schema, t, bucket, t, self.iam_role, self.target_region, delimiter))

    def copy_from_s3(self, bucket, schema, table, creds_path, delimiter=None):
        """
        Copies from an s3 bucket into redshift
        Note that your target_region must be the same region as your connection
        :param bucket: a full bucket path, including the target file
        :param schema: a string of the schema name to be loaded to
        :param table:
        :param creds_path:
        :param delimiter:
        :return:
        """

        if delimiter is None:
            delimiter = '|'

        copy_template = """
        COPY {}.{} FROM 's3://{}000'
        credentials 'aws_iam_role=arn:aws:iam::{}:role/RedshiftCopyUpload'
        region '{}'
        timeformat 'auto'
        delimiter '{}' REMOVEQUOTES ACCEPTANYDATE ESCAPE"""

        engine = self.get_redshift_connection(creds_path).connect()
        trans = engine.begin()

        try:
            engine.execute(copy_template.format(schema, table, bucket, self.iam_role, self.target_region, delimiter))
            trans.commit()
        except:
            trans.rollback()

    def pump_table(self, bucket, source_schema, target_schema, source_creds,
                   target_creds, source_table, target_table,  delimiter=None):
        """
        Pump data from one table to another in a different db using unload and load
        :param bucket:
        :param source_schema:
        :param target_schema:
        :param source_creds:
        :param target_creds:
        :param source_table:
        :param target_table:
        :return: nothing
        """

        if delimiter is None:
            delimiter = '|'

        self.unload_to_s3(bucket, source_schema, source_table, source_creds, delimiter)
        self.copy_from_s3(bucket, target_schema, target_table, target_creds, delimiter)

    def pump_full_schema(self, bucket, source_schema, target_schema, source_creds, target_creds):

        engine = self.get_redshift_connection(source_creds).connect()
        table_query = "select table_name from information_schema.tables where table_schema = '{}"
        tables = pd.read_sql_query(table_query.format(source_schema), engine)

        tables_list = list(tables['table_name'])

        for t in tables_list:
            self.pump_table(bucket, source_schema, target_schema, source_creds, target_creds, t, t)

