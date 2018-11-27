import json
import sqlalchemy as sa


def get_redshift_connection(creds_path):

    """
    Create a sqlalchemy connection to the redshift backup db
    :param creds_path: a string path to a creds json file for creating a sa connection string
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