import os
from collections import defaultdict
import tempfile

from ruamel import yaml

from stride.urbanaccess import create_fake_gtfs

from ..common import now_minus, israel_hour_to_utc_hour
from ..artifacts import upload_artifact, iterate_artifacts


def parse_area_config(id, area_config):
    area_config['id'] = id
    area_config['bbox'] = [float(x.strip()) for x in area_config['bbox'].split(',')]
    return area_config


def read_areas_config():
    with open(os.path.join(os.path.dirname(__file__), 'areas.yaml')) as f:
        return {
            id: parse_area_config(id, config)
            for id, config
            in yaml.safe_load(f).items()
        }


def process_area(area_id, start_hour_israel, end_hour_israel, bbox, stats, limit_stop_times=None,
                 limit_fake_gtfs_processed=None):
    start_hour_utc = israel_hour_to_utc_hour(start_hour_israel)
    end_hour_utc = israel_hour_to_utc_hour(end_hour_israel)
    print(f"Process area {area_id} hours UTC {start_hour_utc:02},{end_hour_utc:02} bbox {bbox} (limit_stop_times={limit_stop_times})")
    for i in range(1, 33):
        date = now_minus(days=i).date()
        artifact_prefix = f'stride-etl/urbanaccess/areas_fake_gtfs/{area_id}-{date.strftime("%Y-%m-%d")}-{start_hour_utc:02}-{end_hour_utc:02}'
        artifacts = list(iterate_artifacts(artifact_prefix, limit=1))
        artifact = artifacts[0] if len(artifacts) > 0 else None
        if artifact and artifact['metadata']['bbox'] != bbox:
            artifact = None
        if not artifact:
            print(f"Create artifact {artifact_prefix}")
            with tempfile.TemporaryDirectory() as fake_gtfs_path:
                create_fake_gtfs.main(date, start_hour_utc, end_hour_utc, ",".join(map(str,bbox)),
                                      target_path=fake_gtfs_path, use_proxy_server=True,
                                      limit_stop_times=limit_stop_times)
                upload_artifact(
                    fake_gtfs_path, artifact_prefix, "",
                    metadata={
                        'area_id': area_id,
                        'start_hour_utc': start_hour_utc,
                        'end_hour_utc': end_hour_utc,
                        'bbox': bbox,
                        'limit_stop_times': limit_stop_times
                    },
                    is_directory=True
                )
                stats['fake_gtfs_processed'] += 1
                if limit_fake_gtfs_processed and stats['fake_gtfs_processed'] >= int(limit_fake_gtfs_processed):
                    break


def main(only_area=None, only_hours=None, limit_stop_times=None, limit_fake_gtfs_processed=None):
    if only_hours:
        only_hours = tuple([int(x.strip()) for x in only_hours.split(',')])
    stats = defaultdict(int)
    for area_id, area_config in read_areas_config().items():
        if not only_area or only_area == area_id:
            for start_hour, end_hour in area_config['hours']:
                if not only_hours or (start_hour, end_hour) == only_hours:
                    process_area(area_id, start_hour, end_hour, area_config['bbox'], stats,
                                 limit_stop_times=limit_stop_times,
                                 limit_fake_gtfs_processed=limit_fake_gtfs_processed)
