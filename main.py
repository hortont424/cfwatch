import re

from collections import defaultdict
from util import open_cloudfront_log_db
from stats import STATS

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

    mapreduce_rows(cloudfront_db, STATS)

if __name__ == "__main__":
    __main__()