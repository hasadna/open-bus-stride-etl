import datetime


def parse_siri_snapshot_id(snapshot_id):
    return datetime.datetime.strptime(snapshot_id + 'z+0000', '%Y/%m/%d/%H/%Mz%z')
