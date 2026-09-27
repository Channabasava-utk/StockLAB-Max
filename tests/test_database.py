import os, tempfile
from database import migrate, connect

def test_migrations_are_idempotent():
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        migrate(path); migrate(path)
        c=connect(path)
        cols={r[1] for r in c.execute("PRAGMA table_info(job_history)").fetchall()}
        assert {"finished_at","payload","result","error"} <= cols
        c.close()
    finally:
        if os.path.exists(path): os.remove(path)
