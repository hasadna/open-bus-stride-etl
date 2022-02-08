import os
import glob
import shutil
import datetime
import tempfile
import subprocess

import pytz

from .. import config


def path_backup(path, path_prefix, backup_path_prefix):
    assert config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID and config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY
    print("Backing up path: {} (path_prefix={} backup_path_prefix={})".format(path, path_prefix, backup_path_prefix))
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.check_call(
            ['tar', '-jcvf', os.path.join(tmpdir, 'backup.tar.bz2'), '.'],
            cwd=path
        )
        for i in range(30):
            if i == 0:
                target_s3_path = f's3://{config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME}/{backup_path_prefix}/{path_prefix}.tar.bz2'
            else:
                target_s3_path = f's3://{config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME}/{backup_path_prefix}/{path_prefix}__{i+1}.tar.bz2'
            if subprocess.call(
                [
                    'aws', 's3', 'ls', target_s3_path
                ],
                env={**os.environ, 'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID, 'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY}
            ) == 1:
                break
            else:
                target_s3_path = None
        assert target_s3_path, 'failed to find available target s3 path for backup'
        print(f'target_s3_path: {target_s3_path}')
        subprocess.check_call(
            [
                'aws', 's3', 'cp',
                os.path.join(tmpdir, 'backup.tar.bz2'), target_s3_path
            ],
            env={**os.environ, 'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID, 'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY}
        )


def main():
    now = datetime.datetime.now(pytz.UTC)
    last_week = now - datetime.timedelta(days=7)
    for cfg in [
        {
            'root_path': config.OPEN_BUS_SIRI_STORAGE_ROOTPATH,
        },
        {
            'root_path': os.path.join(config.OPEN_BUS_SIRI_ETL_ROOTPATH, 'monitored_stop_visits_parse_failed'),
            'backup_path_prefix': 'siri_etl_monitored_stop_visits_parse_failed'
        },
    ]:
        for d in range(30 * 12 * 20):
            path_prefix = (last_week - datetime.timedelta(days=d)).strftime('%Y/%m/%d')
            path = os.path.join(cfg['root_path'], path_prefix)
            if os.path.exists(path):
                if cfg.get('backup_path_prefix'):
                    path_backup(path, path_prefix, cfg['backup_path_prefix'])
                print("Removing path: {}".format(path))
                shutil.rmtree(path)
                path_prefix = (last_week - datetime.timedelta(days=d)).strftime('%Y/%m')
                path = os.path.join(cfg['root_path'], path_prefix)
                if len(glob.glob(path + '/*')) == 0:
                    os.rmdir(path)
                path_prefix = (last_week - datetime.timedelta(days=d)).strftime('%Y')
                path = os.path.join(cfg['root_path'], path_prefix)
                if len(glob.glob(path + '/*')) == 0:
                    os.rmdir(path)
