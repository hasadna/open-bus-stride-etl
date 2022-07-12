import datetime
import traceback
from textwrap import dedent
from pprint import pprint
from collections import defaultdict

from open_bus_stride_db.db import get_session
from open_bus_stride_db.model import GtfsData, GtfsDataTask


def gtfs_data_task_processing_started(date, task_name):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(
            GtfsData.date == date,
            GtfsData.processing_success == True
        ).one_or_none()
        assert gtfs_data
        gtfs_data_task = session.query(GtfsDataTask).filter(
            GtfsDataTask.gtfs_data_id == gtfs_data.id,
            GtfsDataTask.task_name == task_name
        ).one_or_none()
        if gtfs_data_task is None:
            gtfs_data_task = GtfsDataTask(
                gtfs_data_id=gtfs_data.id,
                task_name=task_name,
                started_at=datetime.datetime.now(datetime.timezone.utc),
            )
            session.add(gtfs_data_task)
            session.commit()
            return gtfs_data_task.id
        else:
            gtfs_data_task.started_at = datetime.datetime.now(datetime.timezone.utc)
            gtfs_data_task.completed_at = None
            gtfs_data_task.error = None
            gtfs_data_task.success = None
            session.commit()
            return gtfs_data_task.id


def update_gtfs_data_task(gtfs_data_task_id, error=None, success=None):
    with get_session() as session:
        gtfs_data_task = session.query(GtfsDataTask).filter(GtfsDataTask.id == gtfs_data_task_id).one_or_none()
        assert gtfs_data_task
        gtfs_data_task.completed_at = datetime.datetime.now(datetime.timezone.utc)
        if success:
            assert not error
            gtfs_data_task.success = True
        else:
            assert error
            gtfs_data_task.error = error
            gtfs_data_task.success = False
        session.commit()


def gtfs_data_task_set_success(date, task_name):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(
            GtfsData.date == date,
            GtfsData.processing_success == True
        ).one_or_none()
        assert gtfs_data
        gtfs_data_task = session.query(GtfsDataTask).filter(
            GtfsDataTask.gtfs_data_id == gtfs_data.id,
            GtfsDataTask.task_name == task_name
        ).one_or_none()
        if gtfs_data_task is None:
            session.add(GtfsDataTask(
                gtfs_data_id=gtfs_data.id,
                task_name=task_name,
                success=True
            ))
            session.commit()
        else:
            gtfs_data_task.started_at = None
            gtfs_data_task.completed_at = None
            gtfs_data_task.error = None
            gtfs_data_task.success = True
            session.commit()


def process_date(date, task_name, process_date_function, stats):
    stats['process dates'] += 1
    gtfs_data_task_id = gtfs_data_task_processing_started(date, task_name)
    try:
        process_date_function(date, stats)
    except:
        update_gtfs_data_task(gtfs_data_task_id, error=traceback.format_exc())
        raise
    else:
        update_gtfs_data_task(gtfs_data_task_id, success=True)


def iterate_missing_dates(task_name):
    with get_session() as session:
        for row in session.execute(dedent(f"""
            select * from (
                select date
                from gtfs_data
                where id not in (select gtfs_data_id from gtfs_data_task where task_name = '{task_name}')
                and processing_success is true
                union
                select gtfs_data.date
                from gtfs_data, gtfs_data_task
                where gtfs_data.id = gtfs_data_task.gtfs_data_id
                and gtfs_data_task.task_name = '{task_name}'
                and (gtfs_data_task.success is false or gtfs_data_task.success is null)
                and gtfs_data.processing_success is true
            ) a order by a.date desc
        """)):
            yield row.date


def process_missing_dates(task_name, process_date_function, stats, is_date_missing_function):
    for date in iterate_missing_dates(task_name):
        if is_date_missing_function:
            if is_date_missing_function(date):
                process_date(date, task_name, process_date_function, stats)
                return True
            else:
                gtfs_data_task_set_success(date, task_name)
        else:
            process_date(date, task_name, process_date_function, stats)
            return True
    return False


def main(task_name, process_date_function, is_date_missing_function):
    stats = defaultdict(int)
    while process_missing_dates(task_name, process_date_function, stats, is_date_missing_function):
        pprint(dict(stats))
    pprint(dict(stats))
    print('OK')
