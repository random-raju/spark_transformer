import os
import psycopg2
import traceback

from dotenv import load_dotenv

load_dotenv(f"{os.getcwd()}/new_transformer/local.env")

def pg_handle_update(
        db_cursor, db_connection, data_dict, table, commit=True, unique_key="raw_hash"
    ):
        """
        A function like this is being made with the goal
        to have one agreed upon function for all Postgres updates.
        Important pre-requisites:
            - The input is always a python dictionary
            -
        Params:
            - conn: Psycopg2 connection object
            - cur: Psycopg2 cursor object
            - collection: Name of the Postgres collection
            - table: Name of the table
            - data_dict: The updated raw in the form of a dictionary
            - update: Whether to actually make the update or not
            - append: When you want to append to a value already there in the column
        """
        # Getting the raw hash but ensuring raw hash isn't in the
        # update statement to avoid the hsh_unique constraint conflict
        unique_key_val = data_dict[unique_key]
        data_dict.pop(unique_key)
        columns = data_dict.keys()
        # See https://github.com/mozilla/http-observatory/issues/298
        # for why this IF condition is needed
        if len(columns) > 1:
            skel_columns = f"({', '.join(columns)})"
            skel_percentized_columns = [f"%({col})s" for col in columns]
            skel_values = f"({', '.join(skel_percentized_columns)})"
            skeleton = f"""
                    UPDATE {table}
                    SET {skel_columns} =
                        {skel_values}
                    WHERE {unique_key} = '{unique_key_val}'
                        """
        elif len(columns) == 1:
            skel_column = list(columns)[0]
            skel_value = f"%({skel_column})s"
            skeleton = f"""
                    UPDATE {table}
                    SET {skel_column} =
                        {skel_value}
                    WHERE {unique_key} = '{unique_key_val}'
                        """

        """
        The following is where the real utility should lie (hopefully).
        We simply pass a dictionary here not worried about any
        Postgres statement implementation and let Psycopg2 handle it.
        No tuples, nothing, just a plain old data_dict.
        """
        try:
            db_cursor.execute(skeleton, data_dict)
        except Exception as e:
            traceback.print_exc()
            db_connection.rollback()
            """
            This helper function will actually raise
            an exception and let what's calling it take
            care of what to do
            """
            raise e
        if commit:
            db_connection.commit()

        # return db_connection.cursor.rowcount


def update_batch(batch):

    raw_db_connection = psycopg2.connect(
        database = os.environ.get(f"PG_DB_RAW"),
        user = os.environ.get("PG_USERNAME"),
        password = os.environ.get("PG_PASSWORD"),
        host = os.environ.get(f"PG_HOST_RAW"),
        port = os.environ.get("PG_PORT"),
    )
    # raw_db_connection = psycopg2.connect(
    #     database = 'teal_data_scraped_raw',
    #     user = 'tealindia_data_pipeline',
    #     password = 'Hb3bnnjQTSVVvyKL5KB6sNEPWRkpZW7s',
    #     host = 'teal-data-scraped-raw.cv1c0yuqvqlw.ap-south-1.rds.amazonaws.com',
    #     port = os.environ.get("PG_PORT"),
    # )
    # raw_db_connection = psycopg2.connect(
    #     database='teal_data_scraped_raw',
    #     user='postgres',
    #     password='postgres',
    #     host='localhost',
    #     port='2517',
    # )

    raw_db_cursor = raw_db_connection.cursor()

    for row in batch:
        row = row.asDict()
        table = row.pop('source_table')
        pg_handle_update(raw_db_cursor , raw_db_connection, row, table, unique_key = 'raw_hash')

    raw_db_connection.close()
