"""Durable job metadata store used by the local worker and future queue workers."""
from __future__ import annotations
import json, os, uuid
from datetime import datetime, timezone
from database import connect

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments.db")

def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")

def create(job_type, payload=None):
    jid = uuid.uuid4().hex[:12]
    c = connect(DB_PATH)
    try:
        c.execute("INSERT INTO job_history(id,created_at,status,job_type,payload,result,error) VALUES(?,?,?,?,?,?,?)",
                  (jid, now(), "queued", job_type, json.dumps(payload or {}, default=str), None, None))
        c.commit()
    finally: c.close()
    return jid

def update(jid, status, result=None, error=None):
    c=connect(DB_PATH)
    try:
        c.execute("UPDATE job_history SET status=?,finished_at=?,result=?,error=? WHERE id=?",
                  (status, now() if status in {"completed","failed"} else None,
                   json.dumps(result, default=str) if result is not None else None, error, jid))
        c.commit()
    finally: c.close()

def get(jid):
    c=connect(DB_PATH)
    try:
        r=c.execute("SELECT id,created_at,finished_at,status,job_type,payload,result,error FROM job_history WHERE id=?", (jid,)).fetchone()
    finally: c.close()
    if not r: return None
    return {"id":r[0],"created_at":r[1],"finished_at":r[2],"status":r[3],"job_type":r[4],"payload":json.loads(r[5] or "{}"),"result":json.loads(r[6]) if r[6] else None,"error":r[7]}

def list_jobs(limit=50):
    c=connect(DB_PATH)
    try:
        rows=c.execute("SELECT id,created_at,finished_at,status,job_type,payload,result,error FROM job_history ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    finally: c.close()
    out=[]
    for r in rows:
        out.append({"id":r[0],"created_at":r[1],"finished_at":r[2],"status":r[3],"job_type":r[4],"payload":json.loads(r[5] or "{}"),"result":json.loads(r[6]) if r[6] else None,"error":r[7]})
    return out
