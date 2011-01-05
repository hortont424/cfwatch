from collections import defaultdict
from operator import itemgetter
from heapq import nlargest
from util import format_size

STATS = []

def statistic(f):
    STATS.append(f)

@statistic
def total_bytes():
    def map(row):
        return int(row[1])

    def reduce(results):
        return sum(results)

    def display(result):
        return "Total Transfer: {0}".format(format_size(result))

    return (map, reduce, display)

@statistic
def top_files_requests():
    def map(row):
        return row[6]

    def reduce(results):
        cnt = defaultdict(int)

        for file in results:
            cnt[file] += 1

        return cnt

    def display(result):
        largest_items = nlargest(20, result.iteritems(), itemgetter(1))
        return ("Most Requested Files ({0}):\n   ".format(len(result)) +
                "\n   ".join(["{0} - {1}".format(name, num) for name, num in largest_items]))

    return (map, reduce, display)

@statistic
def top_files_transfer():
    def map(row):
        return (row[6], int(row[1]))

    def reduce(results):
        cnt = defaultdict(int)

        for file, transfer in results:
            cnt[file] += transfer

        return cnt

    def display(result):
        largest_items = nlargest(20, result.iteritems(), itemgetter(1))
        return ("Most Transferred Files ({0}):\n   ".format(len(result)) +
                "\n   ".join(["{0} - {1}".format(name, format_size(num)) for name, num in largest_items]))

    return (map, reduce, display)

@statistic
def transfer_per_bucket():
    def map(row):
        return (row[5], int(row[1]))

    def reduce(results):
        cnt = defaultdict(int)

        for file, transfer in results:
            cnt[file] += transfer

        return cnt

    def display(result):
        largest_items = nlargest(20, result.iteritems(), itemgetter(1))
        return ("Transfer Per Bucket ({0}):\n   ".format(len(result)) +
                "\n   ".join(["{0} - {1}".format(name, format_size(num)) for name, num in largest_items]))

    return (map, reduce, display)

@statistic
def transfer_per_client():
    def map(row):
        return (row[9], int(row[1]))

    def reduce(results):
        cnt = defaultdict(int)

        for file, transfer in results:
            cnt[file] += transfer

        return cnt

    def display(result):
        largest_items = nlargest(5, result.iteritems(), itemgetter(1))
        return ("Transfer Per Client ({0}):\n   ".format(len(result)) +
                "\n   ".join(["{0} - {1}".format(name, format_size(num)) for name, num in largest_items]))

    return (map, reduce, display)