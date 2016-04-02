from babel_util.storage.dynamo import Table, TABLE_DEFINITION, DATASETS
import boto3
import time
import logging
from itertools import groupby
from decimal import Decimal


def id_and_ef(line):
    line = line.split()
    return line[0] + '|' + line[2]


def process_edgelist(stream, rec_type):
    for key, group in groupby(stream, lambda line: id_and_ef(line)):
        key, ef = key.split('|')
        group = map(str.split, group)
        recs = set([s[1] for s in group])
        yield debucketer(key, rec_type, ef, recs)


def debucketer(key, rec_type, ef, recs):
    hash_key = "%s|%s" % (key, rec_type)
    return {TABLE_DEFINITION["hash_key"]: hash_key,
            TABLE_DEFINITION["range_key"]: Decimal(ef),
            TABLE_DEFINITION["rec_attribute"]: recs}


def main(dataset, expert, classic, region, create=False, flush=False, dryrun=False, verbose=False):
    if region == "localhost":
        client = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    else:
        client = boto3.resource('dynamodb')

    t = Table(client, dataset)

    if flush:
        logging.info("Deleting table: " + t.table_name)
        if not dryrun:
            t.delete()

    if create:
        logging.info("Creating table: " + t.table_name)
        if not dryrun:
            t.create(write=2000)

    entries = 0
    start = time.time()

    with t.get_batch_put_context() as batch:
        print("Generating expert recommendations...")
        for expert_rec in process_edgelist(expert, 'expert'):
            if verbose:
                print(expert_rec)
            if not dryrun:
                batch.put_item(expert_rec)
            entries += 1
            if entries % 50000 == 0:
                current_time = time.time()
                current_rate = entries/(current_time - start)
                print("\nProcessed {0:,} entries in {1:.0f} seconds: {2:.2f} entries/sec".format(entries, time.time()-start, entries/(time.time()-start)))
                sys.stdout.flush()

        # Reset for the second pass
        print("Generating classic recommendations...")
        for classic_rec in process_edgelist(classic, 'classic'):
            if verbose:
                print(classic_rec)
            if not dryrun:
                batch.put_item(classic_rec)
            entries += 1
            if entries % 50000 == 0:
                current_time = time.time()
                current_rate = entries/(current_time - start)
                print("\nProcessed {0:,} entries in {1:.0f} seconds: {2:.2f} entries/sec".format(entries, time.time()-start, entries/(time.time()-start)))
                sys.stdout.flush()
    end = time.time()
    print("\nProcessed {0:,} entries in {1:.0f} seconds: {2:.2f} entries/sec".format(entries, end-start, entries/(end-start)))

    if not dryrun:
        t.update_throughput()

if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Transform recommender output to DynamoDB")
    parser.add_argument("dataset", help="Dataset", choices=DATASETS)
    parser.add_argument("expert", help=" expert file to transform", type=argparse.FileType('r'))
    parser.add_argument("classic", help="classic file to transform", type=argparse.FileType('r'))
    parser.add_argument("--region", help="Region to connect to", default="localhost")
    parser.add_argument("-c", "--create", help="create table in database", action="store_true")
    parser.add_argument("-f", "--flush", help="flush database.", action="store_true")
    parser.add_argument("-d", "--dryrun", help="Process data, but don't insert into DB", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    main(args.dataset, args.expert, args.classic, args.region, args.create, args.flush, args.dryrun, args.verbose)
