import sqlite3

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

def open_cloudfront_log_db():
    db = sqlite3.connect("cf.db")
    return db