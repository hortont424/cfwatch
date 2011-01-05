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