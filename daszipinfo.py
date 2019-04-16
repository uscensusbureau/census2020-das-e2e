#!/usr/bin/env python3
#
"""
daszipinfo.py:

This program reads a ZIP file containing the run of the 2020 Disclosure Avoidance System and reports
metadata from the contained files. It also providers a dictionary reader for each of the contained files.

Simson L. Garfinkel, US Census Bureau, 2019
"""

import zipfile
import io


PERS_FIELDS = ['SCHEMA_TYPE_CODE', 'SCHEMA_BUILD_ID', 'TABBLKST', 'TABBLKCOU', 'ENUMDIST',
               'EUID', 'EPNUM', 'RTYPE', 'QREL', 'QSEX', 'QAGE', 'CENHISP', 'CENRACE', 'QSPANX', 'QRACE1', 'QRACE2', 'QRACE3',
               'QRACE4', 'QRACE5', 'QRACE6', 'QRACE7', 'QRACE8', 'CIT']

UNIT_FIELDS = ['SCHEMA_TYPE_CODE', 'SCHEMA_BUILD_ID', 'TABBLKST', 'TABBLKCOU', 'ENUMDIST',
               'EUID', 'RTYPE', 'GQTYPE', 'TEN', 'VACS', 'FINAL_POP', 'HHT', 'HHT2', 'NPF', 'CPLT', 'UPART', 'MULTG', 'HHLDRAGE', 'HHSPAN',
               'HHRACE', 'PAOC', 'P18', 'P60', 'P65', 'P75']


def info_file(zf, fname, fields, max_records):
    if fname is None:
        return

    print(f"{fname}:")
    record = 0
    for line in io.TextIOWrapper(zf.open(fname, mode='r')):
        line = line.strip()
        if line[0] == '#':
            print(line)
            continue
        record += 1
        print(f"Record {record}:")
        print(line)

        d = dict(zip(fields, line.split("|")))
        print(d)
        print("")
        if record >= max_records:
            break
    print("")
    print("")
    print("")


def process(zipfilename, max_records):
    with zipfile.ZipFile(zipfilename) as zf:
        mdf_files = 2 * [None]
        for zi in zf.infolist():
            print(f"{zi.filename:15} {zi.file_size} (uncompressed)")
            if 'MDF_PER.' in zi.filename:
                mdf_files[0] = zi.filename
            elif 'MDF_UNIT.' in zi.filename:
                mdf_files[1] = zi.filename

        info_file(zf, mdf_files[0], PERS_FIELDS, max_records)
        info_file(zf, mdf_files[1], UNIT_FIELDS, max_records)


if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("zipfilename", help="Name of the zip file to process")
    parser.add_argument("-r", "--max_records", type=int, help="Maximum number of records to display from each MDF", default=3)
    args = parser.parse_args()
    process(args.zipfilename, args.max_records)
