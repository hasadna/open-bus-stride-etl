import os
import json
import glob
import zipfile
import tempfile
import traceback
import subprocess
from contextlib import contextmanager

from .. import common, config

from open_bus_stride_db.db import session_decorator
from open_bus_stride_db.model import Artifact, ArtifactStatusEnum


@contextmanager
def compress_directory(source_file_path, is_directory):
    if is_directory:
        assert os.path.isdir(source_file_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            compressed_filename = os.path.join(tmpdir, 'artifact.zip')
            with zipfile.ZipFile(compressed_filename, 'w',
                                 compression=zipfile.ZIP_DEFLATED,
                                 compresslevel=9) as zip_file:
                for filename in glob.glob(f'{source_file_path}/**/*', recursive=True):
                    zip_file.write(filename, os.path.relpath(filename, source_file_path))
            yield compressed_filename, '.zip'
    else:
        yield source_file_path, ''


@session_decorator
def upload_artifact(session, source_file_path, target_file_prefix, target_file_suffix,
                    metadata=None, is_directory=False):
    assert config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID and config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY
    with compress_directory(source_file_path, is_directory) as (source_file_path, extra_suffix):
        target_file_suffix = f'{target_file_suffix}{extra_suffix}'
        artifact = Artifact(
            file_prefix=target_file_prefix,
            status=ArtifactStatusEnum.uploading,
            metadata_json=json.dumps(metadata),
            error='',
            url='',
            created_at=common.now(),
            file_size=os.path.getsize(source_file_path),
        )
        session.add(artifact)
        session.commit()
        try:
            for i in range(30):
                if i == 0:
                    target_s3_path = f's3://{config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME}/artifacts/{target_file_prefix}{target_file_suffix}'
                else:
                    target_s3_path = f's3://{config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME}/artifacts/{target_file_prefix}__{i + 1}{target_file_suffix}'
                if subprocess.call(
                        [
                            'aws', 's3', 'ls', target_s3_path
                        ],
                        env={**os.environ, 'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID,
                             'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY}
                ) == 1:
                    break
                else:
                    target_s3_path = None
            assert target_s3_path, 'failed to find available target s3 path for backup'
            print(f'target_s3_path: {target_s3_path}')
            subprocess.check_call(
                [
                    'aws', 's3', 'cp',
                    source_file_path, target_s3_path
                ],
                env={**os.environ, 'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID,
                     'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY}
            )
        except:
            artifact.status = ArtifactStatusEnum.error
            artifact.error = str(traceback.format_exc())
            session.commit()
            raise
        artifact.status = ArtifactStatusEnum.success
        artifact.url = target_s3_path.replace(
            f's3://{config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME}',
            f'https://{config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME}.s3.eu-west-1.amazonaws.com'
        )
        session.commit()
        print(f'Uploaded successfully: id={artifact.id}, url={artifact.url}')
        return artifact.id, artifact.url


@session_decorator
def iterate_artifacts(session, file_prefix, limit=None):
    query = session.query(Artifact).filter(
        Artifact.file_prefix.like(f'{file_prefix}%'),
        Artifact.status == ArtifactStatusEnum.success
    ).order_by(Artifact.created_at.desc())
    if limit:
        query = query.limit(limit)
    for artifact in query:
        yield {
            'id': artifact.id,
            'file_prefix': artifact.file_prefix,
            'url': artifact.url,
            'created_at': artifact.created_at,
            'metadata': json.loads(artifact.metadata_json),
            'file_size': artifact.file_size,
        }
