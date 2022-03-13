import os
import datetime
import subprocess

from .. import config


def main():
    local_backup_filename = os.path.join(config.OPEN_BUS_SIRI_STORAGE_ROOTPATH, 'stride_db_backup/stride_db.sql.gz')
    assert os.path.exists(local_backup_filename), 'missing local backup file: {}'.format(local_backup_filename)
    remote_backup_path = 's3://{}/stride_db_backups/{}'.format(
        config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME,
        datetime.datetime.now().strftime('%Y/%m/%d/%H%M%S.sql.gz')
    )
    print("Copying from {} to {}".format(local_backup_filename, remote_backup_path))
    subprocess.check_call(
        [
            'aws', 's3', 'cp',
            local_backup_filename, remote_backup_path
        ],
        env={
            **os.environ,
            'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY
        }
    )
