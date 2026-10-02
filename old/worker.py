"""Durable local worker. Replaceable by Celery/RQ/Arq without changing API contracts."""
from __future__ import annotations
import threading, traceback
from job_store import create, update

_jobs = {}
_lock = threading.Lock()

def submit(fn, *args, job_type="research", payload=None, **kwargs):
    jid=create(job_type,payload)
    item={"id":jid,"status":"queued","result":None,"error":None}
    with _lock: _jobs[jid]=item
    def run():
        with _lock: _jobs[jid]["status"]="running"
        update(jid,"running")
        try:
            result=fn(*args,**kwargs)
            with _lock: _jobs[jid].update(status="completed",result=result)
            update(jid,"completed",result=result)
        except Exception as exc:
            with _lock: _jobs[jid].update(status="failed",error=str(exc))
            update(jid,"failed",error=str(exc))
    threading.Thread(target=run,daemon=True,name=f"stocklab-job-{jid}").start()
    return item.copy()

def get(jid):
    with _lock: item=_jobs.get(jid)
    if item: return item.copy()
    from job_store import get as durable_get
    return durable_get(jid)

def list_jobs(limit=50):
    from job_store import list_jobs as durable_list
    return durable_list(limit)
