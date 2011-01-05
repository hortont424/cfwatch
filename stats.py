#!/usr/bin/env python

# Copyright 2011 Tim Horton. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY TIM HORTON "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL TIM HORTON OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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