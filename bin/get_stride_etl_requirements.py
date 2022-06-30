#!/usr/bin/env python3
import sys


def main(stride_etl_commit):
    print(f'-r https://github.com/hasadna/open-bus-stride-etl/raw/{stride_etl_commit}/requirements.txt')
    print(f'https://github.com/hasadna/open-bus-stride-etl/archive/{stride_etl_commit}.zip')
    with open('stride-api-latest-commit.txt', 'r') as f:
        stride_api_latest_commit = f.read().strip()
    print(f'-r https://github.com/hasadna/open-bus-stride-api/raw/{stride_api_latest_commit}/requirements.txt')
    print(f'https://github.com/hasadna/open-bus-stride-api/archive/{stride_api_latest_commit}.zip')
    with open('stride-client-latest-tag.txt', 'r') as f:
        stride_client_latest_tag = f.read().strip()
    print(f'open-bus-stride-client[all]=={stride_client_latest_tag}')


if __name__ == '__main__':
    main(*sys.argv[1:])
