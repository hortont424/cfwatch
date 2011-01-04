import re
import sqlite3

from collections import defaultdict

def open_cloudfront_log_db():
    db = sqlite3.connect("cf.db")
    return db

def format_size(sz):
    abbrevs = (
        (1<<50, "PB"),
        (1<<40, "TB"),
        (1<<30, "GB"),
        (1<<20, "MB"),
        (1<<10, "KB"),
        (1, "B")
    )

    for factor, suffix in abbrevs:
        if sz >= factor:
            break

    return "%.2f %s" % (float(sz) / factor, suffix)

## stats

def total_bytes():
    def map(row):
        return int(row[1])
    def reduce(results):
        return sum(results)
    def display(result):
        return "Total Transfer: {0}".format(format_size(result))

    return (map, reduce, display)

#def map_top_files_requests(db):
#    pass
#
#def map_top_files_transfer(db):
#    pass

def mapreduce_rows(db, stats):
    cur = db.cursor()
    cur.execute("select * from logs")

    results = {}
    map_results = defaultdict(list)
    stats_funcs = {}

    for stat in stats:
        stats_funcs[stat] = stat()

    for row in cur:
        for (stat, (map, reduce, display)) in stats_funcs.items():
            map_results[stat].append(map(row))

    for (stat, (map, reduce, display)) in stats_funcs.items():
        results[stat] = reduce(map_results[stat])
        print display(results[stat])

def __main__():
    cloudfront_db = open_cloudfront_log_db()

    stats = [total_bytes]

    mapreduce_rows(cloudfront_db, stats)

if __name__ == "__main__":
    __main__()
