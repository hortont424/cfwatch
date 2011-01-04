import re
import sqlite3

def open_cloudfront_log_db():
    db = sqlite3.connect('cf.db')
    return db

def format_size(sz):
    abbrevs = (
        (1<<50, 'PB'),
        (1<<40, 'TB'),
        (1<<30, 'GB'),
        (1<<20, 'MB'),
        (1<<10, 'KB'),
        (1, 'B')
    )

    for factor, suffix in abbrevs:
        if sz >= factor:
            break

    return '%.2f %s' % (float(sz) / factor, suffix)

def __main__():
    cloudfront_db = open_cloudfront_log_db()

    cur = cloudfront_db.cursor()
    cur.execute('select * from logs')

    total_bytes = sum([int(row[1]) for row in cur])

    print format_size(total_bytes)

if __name__ == "__main__":
    __main__()
