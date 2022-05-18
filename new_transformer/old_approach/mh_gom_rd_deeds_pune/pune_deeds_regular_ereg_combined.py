import time
from datetime import datetime
from multiprocessing import Process
from multiprocessing import Queue
from profile.analytics import config
from profile.analytics import utils

from psycopg2.errors import UniqueViolation

NO_OF_ITEMS = 10000
PROCS = 3
SLEEP_FOR = 3

TABLE_NAME = 'mh_gom_rd_deeds_pune.pune_deeds_regular_ereg_combined'



def update_synth_table(cursor, profile_packet):




def listen_process_pg_queue(pg_queue):

    db_con = utils.QueryManager("clean")

    while True:
        try:

        except Exception as e:


def process_task_item(task_queue, pg_queue):
    """"""

    while True:
        try:
            synth_dict = task_queue.get()
            synthesized_row = synth_row_to_corresponding_tables(synth_dict)
            pg_queue.put(synthesized_row)

        except Exception as excep:
            print(str(excep), " -- ", synth_dict["clean_row"]["clean_hash"])
            # pass


def monitor_queues(db_obj, task_queue, pg_queue):

    task_queue_size = 0
    pg_queue_size = 0
    synth_items_available = True

    while True:

        get_more_items = (
            (task_queue_size == 0) and (pg_queue_size == 0) and (synth_items_available)
        )

        if get_more_items:

            print(
                f"Fetching more rows from raw. Process started at {str(datetime.now())}"
            )

            synth_status_rows = db_obj.fetch_synth_row_from_synth_table(
                collection="profile", table="synth_status", no_of_items=NO_OF_ITEMS
            )

            print(
                f"Finished fetching more rows from raw. Process finished at {str(datetime.now())}"
            )

            if synth_status_rows:
                for synth_status_row in synth_status_rows:
                    collection = synth_status_row["source_schema"]
                    table = synth_status_row["source_table"]
                    clean_hash = synth_status_row["source_id"]
                    clean_row = db_obj.fetch_clean_row_from_clean_table(
                        collection, table, clean_hash
                    )
                    if clean_row and len(clean_row) >= 1:  # just a check
                        clean_row = clean_row[0]
                    else:
                        continue  # todo
                    synth_dict = {
                        "synth_status": synth_status_row,
                        "clean_row": clean_row,
                    }
                    task_queue.put(synth_dict)
            else:
                synth_items_available = False

        else:
            print("Not fetching more rows from clean right now")
            print("Task Queue Size: ", task_queue_size)
            print("Postgres Queue Size: ", pg_queue_size)

        task_queue_size = task_queue.qsize()
        pg_queue_size = pg_queue.qsize()

        time.sleep(SLEEP_FOR)


if __name__ == "__main__":

    db_obj = utils.QueryManager("raw")

    task_queue = Queue()
    pg_queue = Queue()

    for process_number in range(PROCS):
        proc = Process(target=process_task_item, args=(task_queue, pg_queue))
        proc.start()

    # Now launch the Postgres listener
    proc = Process(target=listen_process_pg_queue, args=(pg_queue,))
    proc.start()
    proc = Process(target=listen_process_pg_queue, args=(pg_queue,))
    proc.start()
    proc = Process(target=listen_process_pg_queue, args=(pg_queue,))
    proc.start()
    proc = Process(target=listen_process_pg_queue, args=(pg_queue,))
    proc.start()

    monitor_queues(db_obj, task_queue, pg_queue)