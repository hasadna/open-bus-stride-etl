#!/usr/bin/env python3
import sys


def main(stride_etl_commit):
    print('-r https://github.com/hasadna/open-bus-stride-etl/raw/{}/requirements.txt'.format(stride_etl_commit))
    print('https://github.com/hasadna/open-bus-stride-etl/archive/{}.zip'.format(stride_etl_commit))


if __name__ == '__main__':
    main(*sys.argv[1:])
