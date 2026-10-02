from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from pydantic import BaseModel, Field
import yfinance as yf
import pandas as pd, numpy as np
from itertools import product
from datetime import datetime, timezone
import sqlite3, json, uuid, os, math, hashlib, urllib.request, urllib.error
from typing import Protocol, Optional, Any
from pathlib import Path
import secrets, base64, threading
from database import connect as db_connect, migrate as db_migrate, info as db_info
from worker import submit as submit_job, get as get_job, list_jobs
from broker import PaperBroker, LiveBrokerBoundary, BrokerOrder
from agent_tools import TOOLS as AGENT_TOOLS

APP_VERSION = "7.0.0-FINAL"
app = FastAPI(title="StockLab MAX Quant Research Platform", version=APP_VERSION)

# Vercel/local browser UI: serve the bundled single-page frontend from the same FastAPI app.
# This keeps the existing index.html frontend and /api/* endpoints on one origin.
@app.get("/", include_in_schema=False)
def frontend():
    return FileResponse(Path(__file__).with_name("index.html"))
ALLOWED_ORIGINS = [x.strip() for x in os.getenv("STOCKLAB_CORS_ORIGINS", "*").split(",") if x.strip()]
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_methods=["*"], allow_headers=["*"])

class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        required = os.getenv("STOCKLAB_API_KEY", "").strip()
        if required and request.url.path not in {"/api/health", "/api/auth/status", "/api/auth/register", "/api/auth/login", "/docs", "/openapi.json", "/redoc"} and request.method != "OPTIONS":
            supplied = request.headers.get("x-api-key", "")
            bearer = request.headers.get("authorization", "")
            session_ok = bearer.startswith("Bearer ") and valid_session(bearer[7:].strip())
            if supplied != required and not session_ok:
                from starlette.responses import JSONResponse
                return JSONResponse({"detail":"Authentication required. Use the configured API key or a valid Bearer session."}, status_code=401)
        return await call_next(request)

app.add_middleware(APIKeyMiddleware)

@app.middleware("http")
async def request_metadata(request: Request, call_next):
    started = datetime.now(timezone.utc)
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    response = await call_next(request)
    elapsed_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000
    response.headers["x-request-id"] = request_id
    response.headers["x-stocklab-version"] = APP_VERSION
    response.headers["server-timing"] = f"app;dur={elapsed_ms:.1f}"
    response.headers["x-content-type-options"] = "nosniff"
    response.headers["x-frame-options"] = "DENY"
    response.headers["referrer-policy"] = "same-origin"
    return response

# Small in-process rate limiter for local/demo deployments. Use a gateway/WAF for production scale.
_rate_lock = threading.Lock()
_rate_buckets = {}
RATE_LIMIT = int(os.getenv("STOCKLAB_RATE_LIMIT", "120"))
RATE_WINDOW = 60

@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if request.url.path in {"/api/health", "/api/health/live", "/api/health/ready"}:
        return await call_next(request)
    ip = request.client.host if request.client else "unknown"
    now = datetime.now(timezone.utc).timestamp()
    with _rate_lock:
        bucket = _rate_buckets.setdefault(ip, [])
        bucket[:] = [t for t in bucket if now - t < RATE_WINDOW]
        if len(bucket) >= RATE_LIMIT:
            from starlette.responses import JSONResponse
            return JSONResponse({"detail":"Rate limit exceeded. Try again shortly."}, status_code=429, headers={"Retry-After":"60"})
        bucket.append(now)
    return await call_next(request)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
# Vercel function storage is ephemeral; PostgreSQL should be used for durable persistence.
# /tmp is writable for transient SQLite fallback and is cleared between instances.
DB_PATH = os.path.join("/tmp" if os.getenv("VERCEL") else APP_DIR, "experiments.db")
DB_INFO = db_info(DB_PATH)
db_migrate(DB_PATH)

def db():
    return db_connect(DB_PATH)


def log_experiment(kind, symbol, period, params, metrics):
    try:
        conn = db()
        conn.execute("INSERT INTO experiments(id,ts,kind,symbol,period,params,metrics) VALUES (?,?,?,?,?,?,?)", (
            str(uuid.uuid4())[:8], datetime.now(timezone.utc).isoformat(timespec="seconds"),
            kind, symbol, period, json.dumps(params, default=str), json.dumps(metrics, default=str)))
        conn.commit(); conn.close()
    except Exception:
        pass

# ----------------------------- DATA -----------------------------
class MarketDataProvider(Protocol):
    name: str
    def history(self, symbol: str, period: str = "5y") -> pd.DataFrame: ...

class YahooMarketDataProvider:
    name = "yfinance"
    point_in_time_ready = False
    def history(self, symbol: str, period: str = "5y") -> pd.DataFrame:
        x = yf.download(symbol, period=period, auto_adjust=True, progress=False, threads=False)
        if x is None or x.empty: raise ValueError(f"No market data returned for {symbol}.")
        if isinstance(x.columns, pd.MultiIndex): x.columns = x.columns.get_level_values(0)
        x.columns = [str(c).title() for c in x.columns]
        needed = ["Open", "High", "Low", "Close", "Volume"]
        missing = [c for c in needed if c not in x.columns]
        if missing: raise ValueError(f"Missing OHLCV columns: {missing}")
        x = x[needed].copy().dropna()
        x.index = pd.to_datetime(x.index).tz_localize(None)
        return x

class LocalCSVProvider:
    name = "csv"
    point_in_time_ready = False
    def history(self, symbol: str, period: str = "5y") -> pd.DataFrame:
        root=os.getenv("STOCKLAB_CSV_DIR",os.path.join(os.path.dirname(os.path.abspath(__file__)),"data"))
        path=os.path.join(root,f"{symbol}.csv")
        if not os.path.exists(path): raise ValueError(f"CSV provider expected {path}")
        x=pd.read_csv(path)
        cols={str(c).strip().lower():c for c in x.columns}
        date_col=cols.get("date") or cols.get("datetime")
        if not date_col: raise ValueError("CSV requires a date column.")
        rename={cols[k]:k.title() for k in ["open","high","low","close","volume"] if k in cols}
        x=x.rename(columns=rename)
        needed=["Open","High","Low","Close","Volume"]
        missing=[c for c in needed if c not in x.columns]
        if missing: raise ValueError(f"CSV missing columns: {missing}")
        x["Date"]=pd.to_datetime(x[date_col])
        x=x.set_index("Date")[needed].sort_index().dropna()
        return x

def make_data_provider():
    return LocalCSVProvider() if os.getenv("STOCKLAB_DATA_PROVIDER","yfinance").lower()=="csv" else YahooMarketDataProvider()

DATA_PROVIDER = make_data_provider()
CACHE_ROOT = os.getenv("STOCKLAB_CACHE_DIR")
if not CACHE_ROOT:
    CACHE_ROOT = "/tmp/stocklab_cache" if os.getenv("VERCEL") else os.path.join(APP_DIR, ".stocklab_cache")
CACHE_DIR = CACHE_ROOT
os.makedirs(CACHE_DIR, exist_ok=True)

def dataset_fingerprint(x: pd.DataFrame) -> str:
    payload = x.reset_index().to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]

def code_fingerprint() -> str:
    try: return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:16]
    except Exception: return APP_VERSION

def data(symbol, period="5y"):
    symbol = str(symbol).strip().upper()
    cache_file = os.path.join(CACHE_DIR, f"{symbol.replace('/','_')}_{period}.csv")
    cached = False
    try:
        if os.getenv("STOCKLAB_CACHE", "1") == "1" and os.path.exists(cache_file):
            x = pd.read_csv(cache_file, parse_dates=[0], index_col=0)
            if len(x) >= 30: cached = True
        else: raise FileNotFoundError
    except Exception:
        x = DATA_PROVIDER.history(symbol, period)
        try: x.to_csv(cache_file)
        except Exception: pass
    x.index = pd.to_datetime(x.index).tz_localize(None)
    fp = dataset_fingerprint(x)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        conn=db(); conn.execute("INSERT INTO data_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (str(uuid.uuid4())[:8],symbol,DATA_PROVIDER.name,"OHLCV",now,str(x.index.min().date()),str(x.index.max().date()),now,fp,len(x),"1.0",0,json.dumps({"cached":cached,"note":"Yahoo historical data is not point-in-time corporate-data adjusted for historical availability."}))); conn.commit(); conn.close()
    except Exception: pass
    return x


def safe_num(v):
    try:
        if v is None or (isinstance(v, float) and not np.isfinite(v)): return None
        return float(v)
    except Exception: return None

def pct(v):
    return None if v is None else float(v) * 100

# ----------------------------- STRATEGY ENGINE -----------------------------
def normalize_params(p):
    p = dict(p)
    defaults = {"fast":20,"slow":100,"ema":50,"rsi":14,"rmin":45,"rmax":70,"exit":75,
                "bb":20,"bbstd":2,"atr":14,"vol":20,"vm":1,"cost":5,"slip":2}
    for k,v in defaults.items(): p.setdefault(k,v)
    return p

def features(x, p):
    p = normalize_params(p); z=x.copy()
    z["SMAF"] = z.Close.rolling(int(p["fast"])).mean()
    z["SMAS"] = z.Close.rolling(int(p["slow"])).mean()
    z["EMA"] = z.Close.ewm(span=int(p["ema"]), adjust=False).mean()
    d=z.Close.diff(); up=d.clip(lower=0).rolling(int(p["rsi"])).mean(); dn=(-d.clip(upper=0)).rolling(int(p["rsi"])).mean()
    z["RSI"] = 100 - 100/(1+up/dn.replace(0,np.nan))
    mid=z.Close.rolling(int(p["bb"])).mean(); sd=z.Close.rolling(int(p["bb"])).std()
    z["BBU"] = mid+float(p["bbstd"])*sd; z["BBL"] = mid-float(p["bbstd"])*sd
    tr=pd.concat([(z.High-z.Low),(z.High-z.Close.shift()).abs(),(z.Low-z.Close.shift()).abs()],axis=1).max(axis=1)
    z["ATR"] = tr.rolling(int(p["atr"])).mean(); z["VOLMA"] = z.Volume.rolling(int(p["vol"])).mean()
    return z.dropna()

def strategy_returns(x,p):
    p=normalize_params(p); z=features(x,p)
    if len(z)<10: raise ValueError("Not enough observations after indicator warm-up.")
    entry=(z.SMAF>z.SMAS)&(z.Close>z.EMA)&(z.RSI>=p["rmin"])&(z.RSI<=p["rmax"])&(z.Volume>=z.VOLMA*p["vm"])
    exit_=(z.SMAF<z.SMAS)|(z.RSI>=p["exit"])|(z.Close<z.BBL)
    pos=pd.Series(0.,index=z.index); hold=False
    for i in range(len(z)):
        if not hold and bool(entry.iloc[i]): hold=True
        elif hold and bool(exit_.iloc[i]): hold=False
        pos.iloc[i]=1 if hold else 0
    held=pos.shift(1).fillna(0); ret=z.Close.pct_change().fillna(0)
    turnover=held.diff().abs().fillna(held.abs()); friction=(float(p["cost"])+float(p["slip"]))/10000
    sr=held*ret-turnover*friction
    return z,held,sr

def metrics_from_returns(sr, bench):
    sr=sr.fillna(0); bench=bench.reindex(sr.index).fillna(0)
    eq=(1+sr).cumprod(); beq=(1+bench).cumprod(); peak=eq.cummax(); dd=eq/peak-1
    std=sr.std(); downside=sr[ sr < 0 ].std()
    sharpe=float(sr.mean()/std*np.sqrt(252)) if std and np.isfinite(std) else 0
    sortino=float(sr.mean()/downside*np.sqrt(252)) if downside and np.isfinite(downside) else 0
    years=max(len(sr)/252,1/252); cagr=float(eq.iloc[-1]**(1/years)-1)
    return {"return":float((eq.iloc[-1]-1)*100),"benchmark":float((beq.iloc[-1]-1)*100),
            "cagr":cagr*100,"sharpe":sharpe,"sortino":sortino,"drawdown":float(dd.min()*100),
            "volatility":float(sr.std()*np.sqrt(252)*100),"win_rate":float((sr>0).mean()*100)}

def bt(x,p,curve=False):
    z,held,sr=strategy_returns(x,p); ret=z.Close.pct_change().fillna(0)
    out=metrics_from_returns(sr,ret); out["trades"]=int((held.diff()>0).sum()); out["exposure"]=float(held.mean()*100)
    if curve:
        eq=(1+sr).cumprod(); bench=(1+ret).cumprod(); step=max(1,len(eq)//180)
        out["equity_curve"]=[{"date":str(d.date()),"strategy":round(float(a),5),"benchmark":round(float(b),5)} for d,a,b in zip(eq.index[::step],eq.values[::step],bench.values[::step])]
    return out

class Req(BaseModel):
    symbol:str="RELIANCE.NS"; period:str="5y"; fast:int=20; slow:int=100; ema:int=50
    rsi:int=14; rmin:float=45; rmax:float=70; exit:float=75; bb:int=20; bbstd:float=2
    atr:int=14; vol:int=20; vm:float=1; cost:float=5; slip:float=2

# ----------------------------- AUTH / JOBS / AGENT BOUNDARIES -----------------------------
def _hash_password(password: str, salt: bytes | None = None):
    salt = salt or secrets.token_bytes(16)
    import hashlib as _hashlib
    dk = _hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 240_000)
    return base64.b64encode(salt + dk).decode()

def _verify_password(password: str, encoded: str):
    try:
        raw = base64.b64decode(encoded.encode()); salt, expected = raw[:16], raw[16:]
        import hashlib as _hashlib
        actual = _hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 240_000)
        return secrets.compare_digest(actual, expected)
    except Exception: return False

def _session_token(): return secrets.token_urlsafe(32)

def valid_session(token: str):
    if not token: return False
    import datetime as _dt
    h=hashlib.sha256(token.encode()).hexdigest()
    conn=db(); row=conn.execute("SELECT expires_at FROM sessions WHERE token_hash=?",(h,)).fetchone(); conn.close()
    if not row: return False
    try: return _dt.datetime.fromisoformat(row[0]) > _dt.datetime.now(timezone.utc)
    except Exception: return False

class AuthPayload(BaseModel):
    email: str
    password: str = Field(min_length=8, max_length=256)

@app.post("/api/auth/register")
def register(payload: AuthPayload):
    email=payload.email.strip().lower()
    if "@" not in email: raise HTTPException(400,"A valid email is required.")
    conn=db()
    try:
        uid=uuid.uuid4().hex[:12]
        conn.execute("INSERT INTO users VALUES (?,?,?,?,?)",(uid,datetime.now(timezone.utc).isoformat(timespec="seconds"),email,_hash_password(payload.password),"researcher")); conn.commit()
    except Exception as exc:
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(409,"Account already exists.")
        raise
    finally: conn.close()
    return login(payload)

@app.post("/api/auth/login")
def login(payload: AuthPayload):
    email=payload.email.strip().lower(); conn=db(); row=conn.execute("SELECT id,password_hash,role FROM users WHERE email=?",(email,)).fetchone(); conn.close()
    if not row or not _verify_password(payload.password,row[1]): raise HTTPException(401,"Invalid credentials.")
    token=_session_token(); import datetime as _dt
    exp=_dt.datetime.now(timezone.utc)+_dt.timedelta(hours=float(os.getenv("STOCKLAB_SESSION_TTL_HOURS","24")))
    conn=db(); conn.execute("INSERT INTO sessions VALUES (?,?,?,?)",(hashlib.sha256(token.encode()).hexdigest(),row[0],datetime.now(timezone.utc).isoformat(timespec="seconds"),exp.isoformat())); conn.commit(); conn.close()
    return {"token":token,"expires_at":exp.isoformat(),"user":{"id":row[0],"email":email,"role":row[2]}}

@app.get("/api/auth/me")
def auth_me(request: Request):
    token=request.headers.get("authorization","").replace("Bearer ","").strip()
    if not valid_session(token): raise HTTPException(401,"Valid Bearer session required.")
    h=hashlib.sha256(token.encode()).hexdigest(); conn=db(); row=conn.execute("SELECT u.id,u.email,u.role,s.expires_at FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=?",(h,)).fetchone(); conn.close()
    return {"id":row[0],"email":row[1],"role":row[2],"expires_at":row[3]}

@app.post("/api/auth/logout")
def logout(request: Request):
    token=request.headers.get("authorization","").replace("Bearer ","").strip(); h=hashlib.sha256(token.encode()).hexdigest(); conn=db(); conn.execute("DELETE FROM sessions WHERE token_hash=?",(h,)); conn.commit(); conn.close(); return {"ok":True}

@app.get("/api/jobs")
def jobs(limit:int=50): return {"jobs":list_jobs(max(1,min(limit,200)))}

@app.get("/api/jobs/{job_id}")
def job_detail(job_id:str):
    x=get_job(job_id)
    if not x: raise HTTPException(404,"Job not found.")
    return x

@app.get("/api/agent/tools")
def agent_tools():
    return {"tools":AGENT_TOOLS,"principle":"The model may select tools, but tool outputs are the source of truth for research metrics."}

@app.post("/api/agent/tool")
def agent_tool(payload:dict):
    name=str(payload.get("name","")).strip(); args=payload.get("arguments") or {}
    if name not in {x["name"] for x in AGENT_TOOLS}: raise HTTPException(400,"Unknown agent tool.")
    symbol=str(args.get("symbol","RELIANCE.NS")); period=str(args.get("period","5y"))
    r=Req(symbol=symbol,period=period)
    if name=="get_quote": return quote(symbol)
    if name=="backtest": return backtest(r)
    if name=="regime_attribution": return regime_attribution(r)
    if name=="research_health": return research_health(r)
    if name=="data_health": return data_health(symbol,period)
    raise HTTPException(400,"Tool implementation missing.")

@app.get("/api/broker/status")
def broker_status():
    return {"paper":{"name":PaperBroker.name,"enabled":True},"live":{"name":LiveBrokerBoundary.name,"enabled":False,"reason":"Live execution requires a separately audited broker adapter and explicit deployment controls."}}

@app.post("/api/broker/paper/order")
def broker_paper_order(payload:dict):
    order=BrokerOrder(str(payload.get("symbol","")),str(payload.get("side","")),float(payload.get("qty",0)),payload.get("price"),"paper")
    if order.side.upper() not in {"BUY","SELL"} or order.qty<=0: raise HTTPException(400,"Invalid paper order.")
    return PaperBroker().place(order)

# ----------------------------- CORE API -----------------------------
@app.get("/api/health")
def health():
    return {"status":"ok","platform":"StockLab MAX","version":APP_VERSION,"database":DB_INFO.backend}

@app.get("/api/auth/status")
def auth_status():
    return {"required":bool(os.getenv("STOCKLAB_API_KEY","")),"mode":"api-key" if os.getenv("STOCKLAB_API_KEY","") else "local-open"}

@app.get("/api/data/providers")
def data_providers():
    return {"active":DATA_PROVIDER.name,"providers":[{"name":"yfinance","type":"remote","point_in_time_ready":False},{"name":"csv","type":"local-import","point_in_time_ready":True,"contract":"<SYMBOL>.csv with date, open, high, low, close, volume"}],"cache_dir":CACHE_DIR,"cache_enabled":os.getenv("STOCKLAB_CACHE","1")=="1"}

@app.get("/api/data/snapshot/{symbol}")
def data_snapshot(symbol:str, period:str="5y"):
    x=data(symbol,period)
    return {"symbol":symbol.upper(),"period":period,"provider":DATA_PROVIDER.name,"rows":len(x),"start":str(x.index.min().date()),"end":str(x.index.max().date()),"fingerprint":dataset_fingerprint(x),"point_in_time_ready":getattr(DATA_PROVIDER,"point_in_time_ready",False),"available_at":"retrieval_time","note":"Historical OHLCV is reusable, but this prototype does not reconstruct the information set known on each historical date."}

@app.get("/api/quote/{symbol}")
def quote(symbol):
    try:
        x=data(symbol,"1mo"); c=float(x.Close.iloc[-1]); prev=float(x.Close.iloc[-2])
        return {"symbol":symbol,"price":c,"change":c-prev,"change_pct":(c/prev-1)*100,"date":str(x.index[-1].date())}
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/backtest")
def backtest(r:Req):
    try:
        o=bt(data(r.symbol,r.period),r.model_dump(),curve=True)
        log_experiment("backtest",r.symbol,r.period,r.model_dump(),{k:v for k,v in o.items() if k!="equity_curve"})
        return o
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/discover")
def discover(r:Req):
    try:
        x=data(r.symbol,r.period); cut=int(len(x)*.7); tr=x.iloc[:cut]; te=x.iloc[cut:]; base=r.model_dump(); candidates=[]
        for f,s,rm,vm in product([10,20,30,40],[60,100,150,200],[40,45,50],[.8,1,1.2]):
            if f>=s: continue
            p=base|{"fast":f,"slow":s,"rmin":rm,"vm":vm}; a=bt(tr,p)
            score=.5*a["sharpe"]+.3*a["return"]/100+.2*max(a["drawdown"],-50)/50
            candidates.append((score,p,a))
        candidates.sort(reverse=True,key=lambda q:q[0]); out=[]
        for score,p,a in candidates[:20]:
            b=bt(te,p); out.append({"fast":p["fast"],"slow":p["slow"],"rmin":p["rmin"],"vm":p["vm"],
                "train_return":round(a["return"],2),"train_sharpe":round(a["sharpe"],2),"test_return":round(b["return"],2),
                "test_sharpe":round(b["sharpe"],2),"test_dd":round(b["drawdown"],2),"trades":b["trades"]})
        log_experiment("discover",r.symbol,r.period,base,{"top_candidate":out[0] if out else {},"candidates_evaluated":len(candidates)})
        return {"train_rows":len(tr),"test_rows":len(te),"results":out}
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/walkforward")
def walkforward(r:Req):
    try:
        x=data(r.symbol,r.period); n=len(x); train=int(n*.5); test=int(n*.15); out=[]; i=0; base=r.model_dump()
        grid=[(f,s) for f,s in product([15,20,30],[80,100,150]) if f<s]
        while i+train+test<=n and len(out)<8:
            tr=x.iloc[i:i+train]; te=x.iloc[i+train:i+train+test]; best=None
            for f,s in grid:
                p=base|{"fast":f,"slow":s}; a=bt(tr,p)
                if best is None or a["sharpe"]>best[0]: best=(a["sharpe"],p)
            p=best[1]; b=bt(te,p)
            out.append({"window":len(out)+1,"fast":p["fast"],"slow":p["slow"],"train_sharpe":round(best[0],2),
                        "test_return":round(b["return"],2),"sharpe":round(b["sharpe"],2),"dd":round(b["drawdown"],2),"trades":b["trades"]}); i+=test
        log_experiment("walkforward",r.symbol,r.period,base,{"windows":len(out),"results":out})
        return {"windows":out}
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/montecarlo")
def montecarlo(r:Req,simulations:int=1000):
    try:
        x=data(r.symbol,r.period); _,_,sr=strategy_returns(x,r.model_dump()); d=sr.values; rng=np.random.default_rng(7); finals=[]; dds=[]
        for _ in range(min(max(simulations,200),3000)):
            s=rng.choice(d,len(d),replace=True); eq=np.cumprod(1+s); pk=np.maximum.accumulate(eq); finals.append(eq[-1]-1); dds.append(np.min(eq/pk-1))
        out={"simulations":len(finals),"median":float(np.median(finals))*100,"p5":float(np.percentile(finals,5))*100,"p95":float(np.percentile(finals,95))*100,"worst_dd":float(np.min(dds))*100}
        log_experiment("montecarlo",r.symbol,r.period,r.model_dump(),out); return out
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/robustness")
def robustness(r:Req):
    try:
        x=data(r.symbol,r.period); p=r.model_dump(); fast_values=[10,15,20,25,30,40]; slow_values=[60,80,100,120,150,200]; rows=[]
        for f,s in product(fast_values,slow_values):
            if f>=s: continue
            o=bt(x,p|{"fast":f,"slow":s}); rows.append({"fast":f,"slow":s,"return":round(o["return"],2),"sharpe":round(o["sharpe"],2),"dd":round(o["drawdown"],2)})
        return {"variants":rows,"fast_values":fast_values,"slow_values":slow_values}
    except Exception as e: raise HTTPException(400,str(e))

# ----------------------------- FUNDAMENTALS / PORTFOLIO -----------------------------
def get_fund_snapshot(symbol):
    t=yf.Ticker(symbol); info={}
    try: info=t.info or {}
    except Exception: info={}
    return {"symbol":symbol,"name":info.get("longName"),"sector":info.get("sector"),"industry":info.get("industry"),
        "marketCap":safe_num(info.get("marketCap")),"pe":safe_num(info.get("trailingPE")),"forwardPE":safe_num(info.get("forwardPE")),
        "pb":safe_num(info.get("priceToBook")),"roe":safe_num(info.get("returnOnEquity")),"roa":safe_num(info.get("returnOnAssets")),
        "de":safe_num(info.get("debtToEquity")),"margin":safe_num(info.get("profitMargins")),"opMargin":safe_num(info.get("operatingMargins")),
        "revGrowth":safe_num(info.get("revenueGrowth")),"earnGrowth":safe_num(info.get("earningsGrowth")),"dividendYield":safe_num(info.get("dividendYield")),
        "beta":safe_num(info.get("beta")),"price":safe_num(info.get("currentPrice")),"high52":safe_num(info.get("fiftyTwoWeekHigh")),"low52":safe_num(info.get("fiftyTwoWeekLow"))}

@app.get("/api/fundamentals/{symbol}")
def fundamentals(symbol):
    try:
        o=get_fund_snapshot(symbol)
        if not any(v is not None for k,v in o.items() if k not in ("symbol","name","sector","industry")): raise ValueError("No fundamentals data available.")
        return o
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/portfolio")
def portfolio(payload:dict):
    try:
        syms=payload.get("symbols",["RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS"]); syms=[s.strip() for s in syms.split(",")] if isinstance(syms,str) else syms
        period=payload.get("period","2y"); rows=[]; rets={}
        for s in syms[:20]:
            x=data(s,period); ret=x.Close.pct_change().dropna(); rets[s]=ret; m=metrics_from_returns(ret,ret)
            rows.append({"symbol":s,"return":round((x.Close.iloc[-1]/x.Close.iloc[0]-1)*100,2),"volatility":round(m["volatility"],2),"sharpe":round(m["sharpe"],2),"sortino":round(m["sortino"],2),"max_dd":round(m["drawdown"],2),"last":round(float(x.Close.iloc[-1]),2)})
        df=pd.DataFrame(rets).dropna(); corr=df.corr().round(2) if len(df.columns)>1 else pd.DataFrame(); matrix=[{"symbol":s,**{c:float(corr.loc[s,c]) for c in corr.columns}} for s in corr.index] if not corr.empty else []
        return {"assets":rows,"correlation":matrix,"symbols":list(corr.columns) if not corr.empty else syms[:20]}
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/optimize")
def optimize(payload:dict):
    try:
        syms=payload.get("symbols",[]); syms=[x.strip() for x in syms.split(",") if x.strip()] if isinstance(syms,str) else syms
        period=payload.get("period","2y"); rets={s:data(s,period).Close.pct_change().dropna() for s in syms[:15]}; df=pd.DataFrame(rets).dropna()
        if df.shape[1]<2: raise ValueError("Need at least two assets with overlapping history.")
        cov=df.cov().values*252; inv=np.linalg.pinv(cov); ones=np.ones(len(df.columns)); w=inv@ones/(ones@inv@ones); w=np.clip(w,0,None); w=w/w.sum(); vol=float(np.sqrt(w@cov@w))
        return {"weights":[{"symbol":s,"weight":round(float(q)*100,2)} for s,q in zip(df.columns,w)],"annualized_volatility":vol*100,"method":"long-only inverse-covariance minimum-variance baseline"}
    except Exception as e: raise HTTPException(400,str(e))

# ----------------------------- REGIME / ML -----------------------------
def market_regime_frame(x):
    z=x.copy(); z["SMA20"]=z.Close.rolling(20).mean(); z["SMA50"]=z.Close.rolling(50).mean(); z["SMA200"]=z.Close.rolling(200).mean(); z["RET20"]=z.Close.pct_change(20); z["VOL20"]=z.Close.pct_change().rolling(20).std()*np.sqrt(252); rows=[]
    for idx,r in z.dropna().iterrows():
        if r.SMA50>r.SMA200 and r.RET20>0 and r.VOL20<.30: reg="Bull / trending"
        elif r.SMA50<r.SMA200 and r.RET20<0: reg="Bear / declining"
        elif r.VOL20>=.30: reg="High volatility"
        else: reg="Sideways / mixed"
        rows.append({"date":str(idx.date()),"regime":reg,"vol":round(float(r.VOL20*100),2),"ret20":round(float(r.RET20*100),2)})
    return rows

@app.post("/api/regime")
def regime(payload:dict):
    try:
        symbol=payload.get("symbol","RELIANCE.NS"); period=payload.get("period","5y"); rows=market_regime_frame(data(symbol,period));
        if not rows: raise ValueError("Not enough data for regime analysis.")
        df=pd.DataFrame(rows); counts=df.regime.value_counts().to_dict(); recent=rows[-1]
        log_experiment("regime",symbol,period,{"method":"SMA50/SMA200 + 20d return + annualized volatility"},counts)
        return {"symbol":symbol,"recent":recent,"counts":counts,"timeline":rows[::max(1,len(rows)//160)]}
    except Exception as e: raise HTTPException(400,str(e))

def ml_features(x):
    z=x.copy(); z["ret1"]=z.Close.pct_change(); z["ret5"]=z.Close.pct_change(5); z["ret20"]=z.Close.pct_change(20); z["vol20"]=z.ret1.rolling(20).std(); z["sma20_gap"]=z.Close/z.Close.rolling(20).mean()-1; z["sma50_gap"]=z.Close/z.Close.rolling(50).mean()-1
    z["rsi14"]=100-100/(1+(z.ret1.clip(lower=0).rolling(14).mean()/(-z.ret1.clip(upper=0)).rolling(14).mean().replace(0,np.nan))); z["target"]=(z.Close.shift(-1)>z.Close).astype(float); return z.dropna()

def sigmoid(v): return 1/(1+np.exp(-np.clip(v,-30,30)))
def fit_logistic(X,y,epochs=700,lr=.04,l2=.02):
    X=np.asarray(X,float); y=np.asarray(y,float); mu=X.mean(0); sd=X.std(0); sd[sd==0]=1; Xs=(X-mu)/sd; w=np.zeros(Xs.shape[1]); b=0.
    for _ in range(epochs):
        p=sigmoid(Xs@w+b); w-=lr*((Xs.T@(p-y))/len(y)+l2*w); b-=lr*float(np.mean(p-y))
    return w,b,mu,sd

@app.post("/api/ml")
def ml_lab(payload:dict):
    try:
        symbol=payload.get("symbol","RELIANCE.NS"); period=payload.get("period","5y"); z=ml_features(data(symbol,period)); cols=["ret1","ret5","ret20","vol20","sma20_gap","sma50_gap","rsi14"]; cut=int(len(z)*.7); tr=z.iloc[:cut]; te=z.iloc[cut:]
        w,b,mu,sd=fit_logistic(tr[cols].values,tr.target.values); probs=sigmoid(((te[cols].values-mu)/sd)@w+b); pred=(probs>=.5).astype(float); acc=float((pred==te.target.values).mean()); tp=int(((pred==1)&(te.target.values==1)).sum()); fp=int(((pred==1)&(te.target.values==0)).sum()); fn=int(((pred==0)&(te.target.values==1)).sum()); precision=tp/max(tp+fp,1); recall=tp/max(tp+fn,1); latest=float(sigmoid(((z.iloc[[-1]][cols].values-mu)/sd)@w+b)[0]); out={"train_rows":len(tr),"test_rows":len(te),"accuracy":acc*100,"precision":precision*100,"recall":recall*100,"latest_up_probability":latest*100,"weights":{c:round(float(v),4) for c,v in zip(cols,w)},"method":"chronological logistic regression baseline"}; log_experiment("ml",symbol,period,{"features":cols,"split":70},out); return out
    except Exception as e: raise HTTPException(400,str(e))

# ----------------------------- SCREENER / FACTORS -----------------------------
def technical_snapshot(symbol):
    x=data(symbol,"1y"); c=float(x.Close.iloc[-1]); sma20=float(x.Close.rolling(20).mean().iloc[-1]); sma50=float(x.Close.rolling(50).mean().iloc[-1]); r=x.Close.pct_change(); vol=float(r.rolling(20).std().iloc[-1]*np.sqrt(252)*100); return {"price":c,"above_sma20":c>sma20,"above_sma50":c>sma50,"volatility":vol}

@app.post("/api/screener")
def screener(payload:dict):
    try:
        symbols=payload.get("symbols",[]); symbols=[x.strip() for x in symbols.split(",") if x.strip()] if isinstance(symbols,str) else symbols; filters=payload.get("filters",{}); out=[]
        for symbol in symbols[:50]:
            try:
                f=get_fund_snapshot(symbol); t=technical_snapshot(symbol); f.update(t); ok=True
                if filters.get("roe_min") is not None and (f["roe"] is None or f["roe"]<filters["roe_min"]/100): ok=False
                if filters.get("de_max") is not None and (f["de"] is None or f["de"]>filters["de_max"]): ok=False
                if filters.get("growth_min") is not None and (f["revGrowth"] is None or f["revGrowth"]<filters["growth_min"]/100): ok=False
                if filters.get("pe_max") is not None and (f["pe"] is None or f["pe"]>filters["pe_max"]): ok=False
                if filters.get("technical")=="uptrend" and not (f["above_sma20"] and f["above_sma50"]): ok=False
                if ok: out.append(f)
            except Exception as ex: out.append({"symbol":symbol,"error":str(ex)})
        log_experiment("screener","MULTI","1y",filters,{"matches":len([x for x in out if "error" not in x])}); return {"results":out,"scanned":len(symbols[:50]),"filters":filters}
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/factors")
def factors(payload:dict):
    try:
        symbols=payload.get("symbols",[]); symbols=[x.strip() for x in symbols.split(",") if x.strip()] if isinstance(symbols,str) else symbols; rows=[]
        for s in symbols[:40]:
            try:
                f=get_fund_snapshot(s); t=technical_snapshot(s)
                quality=np.nanmean([f["roe"] if f["roe"] is not None else np.nan, f["margin"] if f["margin"] is not None else np.nan, 1/(1+max(f["de"] or 0,0))])
                growth=np.nanmean([f["revGrowth"] if f["revGrowth"] is not None else np.nan, f["earnGrowth"] if f["earnGrowth"] is not None else np.nan])
                value=1/(1+max(f["pe"] or 0,0)) if f["pe"] is not None and f["pe"]>0 else np.nan
                momentum=(1 if t["above_sma20"] else 0)+(1 if t["above_sma50"] else 0)
                rows.append({"symbol":s,"quality":None if np.isnan(quality) else round(float(quality)*100,2),"growth":None if np.isnan(growth) else round(float(growth)*100,2),"value":None if np.isnan(value) else round(float(value)*100,2),"momentum":momentum*50,"roe":pct(f["roe"]),"pe":f["pe"]})
            except Exception as e: rows.append({"symbol":s,"error":str(e)})
        return {"results":rows,"note":"Factor values are transparent diagnostics, not a ranking or investment recommendation."}
    except Exception as e: raise HTTPException(400,str(e))

# ----------------------------- RESEARCH COPILOT / ORCHESTRATOR -----------------------------
def parse_hypothesis(text):
    t=text.lower(); fundamental=[]; technical=[]
    if any(k in t for k in ["roe","quality","profitable"]): fundamental.append("ROE > 15%")
    if any(k in t for k in ["debt","low leverage"]): fundamental.append("Debt/Equity < 1")
    if any(k in t for k in ["growth","growing","earnings"]): fundamental.append("Revenue/Earnings growth > 10%")
    if any(k in t for k in ["cheap","valuation","pe"]): fundamental.append("Inspect P/E and P/B")
    if any(k in t for k in ["trend","uptrend","momentum"]): technical.append("Fast SMA > Slow SMA and price above EMA")
    if "rsi" in t: technical.append("RSI constraint")
    if "volume" in t: technical.append("Volume confirmation")
    if not fundamental and not technical: technical.append("Transparent trend + momentum baseline")
    return fundamental,technical

@app.post("/api/hypothesis")
def hypothesis(payload:dict):
    f,t=parse_hypothesis(str(payload.get("text",""))); return {"fundamental_filters":f,"technical_filters":t,"research_steps":["Formalize hypothesis","Define universe","Separate development and unseen data","Backtest explicit rules","Walk-forward re-optimize","Inspect parameter stability","Stress test returns and drawdowns","Analyze regime dependence","Record experiment and limitations"]}

def local_agent_plan(hypothesis, symbol, period):
    f,t=parse_hypothesis(hypothesis)
    return {"agent_mode":"local-rule-engine","formalized":{"fundamental":f,"technical":t},"research_plan":["Define universe and data availability","Create explicit baseline rules","Chronological train/test split","Walk-forward re-optimization","Robustness and transaction-cost sensitivity","Regime-conditioned diagnostics","Monte Carlo path stress","Persist provenance and experiment"],"cautions":["Do not treat model output as a buy/sell instruction.","Historical data may not be point-in-time clean.","ML output requires out-of-sample validation."]}

def llm_call(hypothesis, symbol, period):
    key=os.getenv("LLM_API_KEY","").strip()
    if not key: return local_agent_plan(hypothesis,symbol,period) | {"llm_configured":False}
    base=os.getenv("LLM_BASE_URL","https://api.openai.com/v1").rstrip("/")
    model=os.getenv("LLM_MODEL","gpt-5.6-luna")
    prompt=f"You are a neutral quantitative research assistant. Do not recommend trades. Formalize this hypothesis into testable research questions, variables, controls, validation steps, failure modes, and a concise experiment plan. Symbol: {symbol}. Period: {period}. Hypothesis: {hypothesis}"
    payload={"model":model,"input":[{"role":"system","content":"Return structured research guidance only; distinguish facts, assumptions, and tests."},{"role":"user","content":prompt}],"max_output_tokens":1200}
    req=urllib.request.Request(base+"/responses",data=json.dumps(payload).encode(),headers={"Authorization":"Bearer "+key,"Content-Type":"application/json"},method="POST")
    try:
        with urllib.request.urlopen(req,timeout=45) as r: raw=json.loads(r.read().decode())
        text=raw.get("output_text") or ""
        if not text:
            for item in raw.get("output",[]):
                for c in item.get("content",[]):
                    if c.get("type")=="output_text": text += c.get("text","")
        return {"agent_mode":"live-llm","llm_configured":True,"model":model,"response":text,"formalized":local_agent_plan(hypothesis,symbol,period)["formalized"],"cautions":["LLM output is research assistance, not investment advice.","Verify every claim against the platform's data and experiment results."]}
    except Exception as e:
        return local_agent_plan(hypothesis,symbol,period) | {"llm_configured":True,"llm_error":str(e),"fallback":True}

@app.get("/api/llm/status")
def llm_status():
    return {"configured":bool(os.getenv("LLM_API_KEY","")),"provider":"OpenAI-compatible Responses API","model":os.getenv("LLM_MODEL","gpt-5.6-luna"),"base_url":os.getenv("LLM_BASE_URL","https://api.openai.com/v1"),"safe_mode":True}

@app.post("/api/research-agent/run")
def research_agent_run(payload:dict):
    hypothesis=str(payload.get("hypothesis","")).strip(); symbol=str(payload.get("symbol","RELIANCE.NS")); period=str(payload.get("period","5y"))
    if not hypothesis: raise HTTPException(400,"Hypothesis is required.")
    out=llm_call(hypothesis,symbol,period)
    rid=str(uuid.uuid4())[:8]
    try:
        conn=db(); conn.execute("INSERT INTO research_runs VALUES (?,?,?,?,?,?,?,?,?,?)",(rid,datetime.now(timezone.utc).isoformat(timespec="seconds"),"local-user",hypothesis,symbol,period,None,code_fingerprint(),"planned",json.dumps(out,default=str))); conn.commit(); conn.close()
    except Exception: pass
    out["run_id"]=rid; return out

@app.post("/api/research/run")
def research_run(payload:dict):
    try:
        text=str(payload.get("hypothesis","")); symbol=str(payload.get("symbol","RELIANCE.NS")); period=str(payload.get("period","5y")); f,t=parse_hypothesis(text)
        p=normalize_params(payload.get("params",{})); x=data(symbol,period); cut=int(len(x)*.7); tr=x.iloc[:cut]; te=x.iloc[cut:]
        train=bt(tr,p); test=bt(te,p); full=bt(x,p); regimes=market_regime_frame(x); counts=pd.Series([r["regime"] for r in regimes]).value_counts().to_dict() if regimes else {}
        _,_,sr=strategy_returns(x,p); rng=np.random.default_rng(7); finals=[]
        for _ in range(500):
            s=rng.choice(sr.values,len(sr),replace=True); finals.append(np.cumprod(1+s)[-1]-1)
        health={"oos_return_gap":round(test["return"]-train["return"],2),"oos_sharpe_gap":round(test["sharpe"]-train["sharpe"],2),"trade_count":full["trades"],"cost_sensitivity_note":"Increase friction and re-run to inspect degradation."}
        out={"hypothesis":text,"formalized":{"fundamental":f,"technical":t},"train":train,"test":test,"full":full,"regimes":counts,"montecarlo":{"p5":float(np.percentile(finals,5)*100),"median":float(np.median(finals)*100),"p95":float(np.percentile(finals,95)*100)},"research_health":health,"limitations":["Historical performance is not a guarantee of future results.","Data quality, survivorship and corporate-action handling depend on the data source.","Daily execution is simplified and does not model full market impact or liquidity."]}
        log_experiment("research_run",symbol,period,{"hypothesis":text,"params":p},out); return out
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/research-health")
def research_health(r:Req):
    try:
        x=data(r.symbol,r.period); cut=int(len(x)*.7); tr=x.iloc[:cut]; te=x.iloc[cut:]; a=bt(tr,r.model_dump()); b=bt(te,r.model_dump()); full=bt(x,r.model_dump());
        robust_rows=[]
        for f,s in [(max(10,r.fast-10),r.slow),(r.fast,r.slow),(r.fast+10,r.slow),(r.fast,max(r.fast+20,r.slow-20)),(r.fast,r.slow+20)]:
            if f<s:
                m=bt(x,r.model_dump()|{"fast":f,"slow":s}); robust_rows.append(m["sharpe"])
        stability=float(np.std(robust_rows)) if robust_rows else None
        return {"train":a,"test":b,"full":full,"oos_return_gap":round(b["return"]-a["return"],2),"oos_sharpe_gap":round(b["sharpe"]-a["sharpe"],2),"parameter_sharpe_std":None if stability is None else round(stability,3),"diagnostics":["Large train-to-test degradation deserves investigation.","Low trade counts make metrics less informative.","Parameter instability can indicate sensitivity or overfitting.","Re-run with different periods and universes before drawing conclusions."]}
    except Exception as e: raise HTTPException(400,str(e))

# ----------------------------- EXPERIMENTS / THESES / PAPER -----------------------------
@app.get("/api/experiments")
def experiments(limit:int=50):
    try:
        conn=db(); rows=conn.execute("SELECT id,ts,kind,symbol,period,params,metrics FROM experiments ORDER BY ts DESC LIMIT ?",(min(limit,200),)).fetchall(); conn.close()
        return {"experiments":[{"id":r[0],"ts":r[1],"kind":r[2],"symbol":r[3],"period":r[4],"params":json.loads(r[5]),"metrics":json.loads(r[6])} for r in rows]}
    except Exception as e: raise HTTPException(400,str(e))

@app.get("/api/experiments/{eid}")
def experiment_detail(eid:str):
    conn=db(); r=conn.execute("SELECT id,ts,kind,symbol,period,params,metrics FROM experiments WHERE id=?",(eid,)).fetchone(); conn.close()
    if not r: raise HTTPException(404,"Experiment not found.")
    return {"id":r[0],"ts":r[1],"kind":r[2],"symbol":r[3],"period":r[4],"params":json.loads(r[5]),"metrics":json.loads(r[6])}

@app.get("/api/theses")
def get_theses(limit:int=30):
    conn=db(); rows=conn.execute("SELECT id,ts,symbol,title,thesis,risks,catalysts,status FROM theses ORDER BY ts DESC LIMIT ?",(limit,)).fetchall(); conn.close()
    return {"theses":[{"id":r[0],"ts":r[1],"symbol":r[2],"title":r[3],"thesis":r[4],"risks":r[5],"catalysts":r[6],"status":r[7]} for r in rows]}

@app.post("/api/theses")
def save_thesis(payload:dict):
    try:
        tid=str(uuid.uuid4())[:8]; conn=db(); conn.execute("INSERT INTO theses VALUES (?,?,?,?,?,?,?,?)",(tid,datetime.now(timezone.utc).isoformat(timespec="seconds"),payload.get("symbol",""),payload.get("title","Untitled"),payload.get("thesis",""),payload.get("risks",""),payload.get("catalysts",""),payload.get("status","Active"))); conn.commit(); conn.close(); return {"id":tid}
    except Exception as e: raise HTTPException(400,str(e))

def _paper_state():
    conn=db(); rows=conn.execute("SELECT symbol,side,qty,price FROM paper_orders ORDER BY ts ASC").fetchall(); conn.close()
    cash=1000000.0; pos={}; cost={}; realized=0.0
    for sym,side,qty,price in rows:
        pos[sym]=pos.get(sym,0.0); cost[sym]=cost.get(sym,0.0)
        if side=="BUY":
            cash -= qty*price; pos[sym]+=qty; cost[sym]+=qty*price
        else:
            if qty > pos[sym] + 1e-9: continue
            avg=cost[sym]/pos[sym] if pos[sym] else price
            cash += qty*price; realized += (price-avg)*qty; pos[sym]-=qty; cost[sym]-=avg*qty
    return cash,pos,cost,realized

@app.post("/api/paper/order")
def paper_order(payload:dict):
    try:
        symbol=str(payload["symbol"]).upper(); side=str(payload["side"]).upper(); qty=float(payload["qty"])
        if side not in ("BUY","SELL") or qty<=0: raise ValueError("Use BUY/SELL and positive quantity.")
        price=float(quote(symbol)["price"]); cash,pos,cost,realized=_paper_state()
        if side=="BUY" and qty*price > cash + 1e-9:
            raise ValueError(f"Insufficient paper cash. Available: ₹{cash:,.2f}")
        if side=="SELL" and qty > pos.get(symbol,0.0) + 1e-9:
            raise ValueError(f"Cannot sell {qty:g} shares; paper position is {pos.get(symbol,0):g}.")
        oid=str(uuid.uuid4())[:8]; conn=db(); conn.execute("INSERT INTO paper_orders VALUES (?,?,?,?,?,?,?)",(oid,datetime.now(timezone.utc).isoformat(timespec="seconds"),symbol,side,qty,price,payload.get("note",""))); conn.commit(); conn.close(); return {"id":oid,"symbol":symbol,"side":side,"qty":qty,"price":price,"cash_after_estimate":cash-qty*price if side=="BUY" else cash+qty*price}
    except Exception as e: raise HTTPException(400,str(e))

@app.get("/api/paper")
def paper_book():
    conn=db(); rows=conn.execute("SELECT id,ts,symbol,side,qty,price,note FROM paper_orders ORDER BY ts ASC").fetchall(); conn.close(); cash,pos,cost,realized=_paper_state()
    holdings=[]; market=0
    for sym,qty in pos.items():
        if qty>1e-9:
            try: price=float(quote(sym)["price"]); market+=qty*price; holdings.append({"symbol":sym,"qty":qty,"avg_cost":cost[sym]/qty,"price":price,"market_value":qty*price,"unrealized":(price-cost[sym]/qty)*qty})
            except: holdings.append({"symbol":sym,"qty":qty,"avg_cost":cost[sym]/qty,"price":None,"market_value":None,"unrealized":None})
    return {"orders":[{"id":r[0],"ts":r[1],"symbol":r[2],"side":r[3],"qty":r[4],"price":r[5],"note":r[6]} for r in rows[::-1]],"holdings":holdings,"realized_pnl":realized,"market_value":market,"cash":cash,"equity":cash+market,"starting_cash":1000000.0}

# ----------------------------- REPORT / DATA HEALTH -----------------------------
@app.get("/api/data-health/{symbol}")
def data_health(symbol,period="5y"):
    try:
        x=data(symbol,period); gaps=x.index.to_series().diff().dt.days.dropna(); ret=x.Close.pct_change().dropna(); return {"symbol":symbol,"rows":len(x),"start":str(x.index[0].date()),"end":str(x.index[-1].date()),"missing_cells":int(x.isna().sum().sum()),"duplicate_dates":int(x.index.duplicated().sum()),"max_calendar_gap_days":int(gaps.max()) if len(gaps) else 0,"return_mean":float(ret.mean()*252*100),"return_vol":float(ret.std()*np.sqrt(252)*100),"zero_volume_days":int((x.Volume==0).sum())}
    except Exception as e: raise HTTPException(400,str(e))

@app.get("/api/report/{symbol}")
def report(symbol,period="5y"):
    try:
        p=Req(symbol=symbol,period=period); x=data(symbol,period); b=bt(x,p.model_dump()); regimes=market_regime_frame(x); counts=pd.Series([r["regime"] for r in regimes]).value_counts().to_dict() if regimes else {}; f=get_fund_snapshot(symbol); health=data_health(symbol,period)
        return {"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),"symbol":symbol,"period":period,"backtest":b,"regimes":counts,"fundamentals":f,"data_health":health,"limitations":["Historical backtests are not forecasts.","yfinance coverage varies by ticker and exchange.","Daily execution is simplified; liquidity, spread and market impact are not fully modeled.","Fundamental snapshots can be incomplete or stale relative to a dedicated market-data vendor."]}
    except Exception as e: raise HTTPException(400,str(e))

# ============================= v5 FLAGSHIP EXTENSIONS =============================
# These extensions are intentionally dependency-light and preserve the local-first workflow.
APP_VERSION = "7.0.0-FINAL"
app.version = APP_VERSION
CACHE_ROOT = os.getenv("STOCKLAB_CACHE_DIR")
if not CACHE_ROOT:
    CACHE_ROOT = "/tmp/stocklab_cache" if os.getenv("VERCEL") else os.path.join(APP_DIR, ".stocklab_cache")
CACHE_DIR = CACHE_ROOT
os.makedirs(CACHE_DIR, exist_ok=True)

# ---------- Research provenance / experiment schema ----------
import hashlib

def dataset_fingerprint(x):
    """Stable fingerprint of the exact OHLCV frame used by a research run."""
    payload = x.copy()
    payload.index = pd.to_datetime(payload.index)
    raw = pd.util.hash_pandas_object(payload, index=True).values.tobytes()
    return hashlib.sha256(raw).hexdigest()[:16]

def code_fingerprint():
    try:
        with open(__file__, "rb") as fh:
            return hashlib.sha256(fh.read()).hexdigest()[:16]
    except Exception:
        return "unknown"

def log_experiment_v5(kind, symbol, period, params, metrics, *, x=None, universe=None, tags=None, notes=None):
    """Append an auditable experiment record without breaking the older schema/API."""
    try:
        conn = db()
        eid = str(uuid.uuid4())[:8]
        conn.execute("""INSERT INTO experiments
            (id,ts,kind,symbol,period,params,metrics,dataset_fingerprint,code_version,universe,tags,notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
            eid, datetime.now(timezone.utc).isoformat(timespec="seconds"), kind, symbol, period,
            json.dumps(params, default=str), json.dumps(metrics, default=str),
            dataset_fingerprint(x) if x is not None else None, code_fingerprint(),
            json.dumps(universe, default=str) if universe is not None else None,
            json.dumps(tags, default=str) if tags is not None else None, notes))
        conn.commit(); conn.close()
        return eid
    except Exception:
        return None

# ---------- Data provider abstraction + local freshness cache ----------
class MarketDataProvider:
    name = "abstract"
    def history(self, symbol, period="5y"):
        raise NotImplementedError

class YFinanceProvider(MarketDataProvider):
    name = "yfinance"
    def history(self, symbol, period="5y"):
        return _download_yfinance(symbol, period)

def _cache_key(symbol, period):
    return hashlib.sha256(f"{symbol.upper()}::{period}".encode()).hexdigest()[:24]

def _download_yfinance(symbol, period):
    x = yf.download(symbol, period=period, auto_adjust=True, progress=False, threads=False)
    if x is None or x.empty:
        raise ValueError(f"No market data returned for {symbol}.")
    if isinstance(x.columns, pd.MultiIndex): x.columns = x.columns.get_level_values(0)
    x.columns = [str(c).title() for c in x.columns]
    needed = ["Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in needed if c not in x.columns]
    if missing: raise ValueError(f"Missing OHLCV columns: {missing}")
    x = x[needed].copy().dropna()
    x.index = pd.to_datetime(x.index).tz_localize(None)
    return x

def data_cached(symbol, period="5y", max_age_hours=6):
    """Research cache: reproducibility + lower repeated network load, while allowing refresh."""
    symbol = str(symbol).strip().upper()
    path = os.path.join(CACHE_DIR, _cache_key(symbol, period) + ".csv")
    meta = path + ".json"
    now = datetime.now(timezone.utc).timestamp()
    if os.path.exists(path) and os.path.exists(meta):
        try:
            m = json.load(open(meta, "r", encoding="utf-8"))
            if now - float(m.get("created", 0)) <= max_age_hours * 3600:
                z = pd.read_csv(path, parse_dates=["Date"], index_col="Date")
                return z, {"provider": m.get("provider", "yfinance"), "cached": True, "cache_age_hours": (now-float(m.get("created", now)))/3600}
        except Exception:
            pass
    z = YFinanceProvider().history(symbol, period)
    z.to_csv(path, index_label="Date")
    json.dump({"created": now, "provider": "yfinance", "symbol": symbol, "period": period, "rows": len(z)}, open(meta, "w", encoding="utf-8"))
    return z, {"provider": "yfinance", "cached": False, "cache_age_hours": 0}

# ---------- Regime-conditioned strategy diagnostics ----------
def regime_strategy_attribution(x, p):
    z, held, sr = strategy_returns(x, p)
    regime_rows = market_regime_frame(x)
    rg = pd.DataFrame(regime_rows)
    if rg.empty:
        return {"regimes": {}, "overall": bt(x, p), "method": "no regime observations"}
    rg["date"] = pd.to_datetime(rg["date"])
    frame = pd.DataFrame({"sr": sr}).join(rg.set_index("date")["regime"], how="inner")
    out = {}
    for name, g in frame.groupby("regime"):
        out[name] = metrics_from_returns(g.sr, pd.Series(0.0, index=g.index)) | {"days": int(len(g)), "exposure": float((held.reindex(g.index).fillna(0)).mean()*100)}
    return {"regimes": out, "overall": bt(x, p), "method": "strategy daily returns grouped by contemporaneous market-regime labels"}

@app.post("/api/regime-attribution")
def regime_attribution(r: Req):
    try:
        x = data_cached(r.symbol, r.period)[0]
        out = regime_strategy_attribution(x, r.model_dump())
        log_experiment_v5("regime_attribution", r.symbol, r.period, r.model_dump(), out, x=x, tags=["regime", "conditional-performance"])
        return out
    except Exception as e:
        raise HTTPException(400, str(e))

# ---------- Event / corporate-calendar intelligence ----------
@app.get("/api/events/{symbol}")
def events(symbol: str):
    """Best-effort event calendar from yfinance; availability varies by ticker."""
    try:
        t = yf.Ticker(symbol.upper())
        events_out = []
        try:
            cal = t.calendar
            if isinstance(cal, pd.DataFrame):
                cal = cal.to_dict()
            if isinstance(cal, dict):
                for k, v in cal.items():
                    events_out.append({"type": str(k), "value": str(v)})
        except Exception:
            pass
        try:
            ed = t.get_earnings_dates(limit=12)
            if ed is not None and not ed.empty:
                for idx, row in ed.iterrows():
                    events_out.append({"type": "earnings_date", "value": str(idx), "reported_eps": safe_num(row.get("Reported EPS")), "surprise_pct": safe_num(row.get("Surprise(%)"))})
        except Exception:
            pass
        return {"symbol": symbol.upper(), "events": events_out[:40], "source": "yfinance", "note": "Event coverage depends on upstream availability and should be independently verified before decisions."}
    except Exception as e:
        raise HTTPException(400, str(e))

# ---------- Research agent: deterministic local planner + LLM-ready prompt package ----------
def build_research_agent_plan(hypothesis, symbol, period, constraints=None):
    fundamental, technical = parse_hypothesis(hypothesis)
    constraints = constraints or {}
    return {
        "agent_mode": "local-explainable-planner",
        "objective": hypothesis,
        "asset": symbol,
        "period": period,
        "constraints": constraints,
        "formalized": {"fundamental": fundamental, "technical": technical},
        "workflow": [
            "1. Verify data availability and quality",
            "2. Define the investable universe explicitly",
            "3. Translate the hypothesis into deterministic rules",
            "4. Separate development and unseen data chronologically",
            "5. Run baseline and cost-aware backtests",
            "6. Re-optimize only inside training windows",
            "7. Inspect parameter stability and trade count",
            "8. Attribute behavior by market regime",
            "9. Stress strategy returns with bootstrap simulation",
            "10. Store the complete experiment provenance",
            "11. Generate a research dossier with assumptions and limitations",
        ],
        "llm_prompt": (
            "You are a neutral quantitative research assistant. Convert the user's hypothesis into "
            "explicit, testable research rules. Never output a BUY/SELL instruction. State assumptions, "
            "data requirements, leakage risks, confounders, validation design, and failure cases. "
            f"Hypothesis: {hypothesis}"
        )
    }

@app.post("/api/research-agent/plan")
def research_agent_plan(payload: dict):
    try:
        out = build_research_agent_plan(str(payload.get("hypothesis", "")), str(payload.get("symbol", "RELIANCE.NS")), str(payload.get("period", "5y")), payload.get("constraints", {}))
        return out
    except Exception as e:
        raise HTTPException(400, str(e))

# ---------- Advanced report: provenance + conditional analysis + event context ----------
@app.get("/api/research-dossier/{symbol}")
def research_dossier(symbol: str, period: str = "5y"):
    try:
        p = Req(symbol=symbol, period=period)
        x, source = data_cached(symbol, period)
        b = bt(x, p.model_dump(), curve=True)
        rh = research_health(p)
        ra = regime_strategy_attribution(x, p.model_dump())
        dh = data_health(symbol, period)
        f = get_fund_snapshot(symbol)
        ev = events(symbol)
        return {
            "schema_version": "6.1",
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "provenance": {"provider": source["provider"], "cached": source["cached"], "dataset_fingerprint": dataset_fingerprint(x), "code_version": code_fingerprint()},
            "symbol": symbol.upper(), "period": period,
            "backtest": b, "research_health": rh, "regime_attribution": ra,
            "fundamentals": f, "data_health": dh, "events": ev.get("events", []),
            "limitations": [
                "This dossier is a research artifact, not a trading instruction.",
                "Upstream market and fundamental data may be incomplete, revised or stale.",
                "Corporate actions, delistings, survivorship and point-in-time fundamentals require a dedicated institutional data source for production use.",
                "Daily backtests simplify execution, liquidity, spread and market impact.",
            ]
        }
    except Exception as e:
        raise HTTPException(400, str(e))

# ---------- Research snapshot export (portable JSON, no extra dependency) ----------
@app.get("/api/export/experiment/{eid}")
def export_experiment(eid: str):
    conn = db(); r = conn.execute("SELECT id,ts,kind,symbol,period,params,metrics,dataset_fingerprint,code_version,universe,tags,notes FROM experiments WHERE id=?", (eid,)).fetchone(); conn.close()
    if not r: raise HTTPException(404, "Experiment not found.")
    keys = ["id","ts","kind","symbol","period","params","metrics","dataset_fingerprint","code_version","universe","tags","notes"]
    out = dict(zip(keys, r))
    for k in ["params","metrics","universe","tags"]:
        if out[k]:
            try: out[k] = json.loads(out[k])
            except Exception: pass
    out["schema_version"] = "5.0"
    out["exported_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return out

@app.get("/api/health/detailed")
def detailed_health():
    checks = {}
    try:
        conn = db(); conn.execute("SELECT 1").fetchone(); conn.close(); checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {e}"
    try:
        checks["data_provider"] = getattr(DATA_PROVIDER, "name", "unknown")
        checks["cache"] = os.path.isdir(CACHE_DIR)
    except Exception as e:
        checks["data_provider"] = f"error: {e}"
    return {"status": "ok" if checks.get("database") == "ok" else "degraded", "version": APP_VERSION, "checks": checks, "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds")}

@app.get("/api/experiments/compare")
def compare_experiments(ids: str):
    wanted = [x.strip() for x in ids.split(",") if x.strip()][:20]
    if not wanted: raise HTTPException(400, "Provide comma-separated experiment IDs.")
    conn=db(); rows=[]
    for eid in wanted:
        r=conn.execute("SELECT id,ts,kind,symbol,period,metrics,dataset_fingerprint,code_version FROM experiments WHERE id=?",(eid,)).fetchone()
        if r:
            try: metrics=json.loads(r[5]) if r[5] else {}
            except Exception: metrics={}
            rows.append({"id":r[0],"ts":r[1],"kind":r[2],"symbol":r[3],"period":r[4],"metrics":metrics,"dataset_fingerprint":r[6],"code_version":r[7]})
    conn.close()
    return {"count":len(rows),"experiments":rows,"note":"Comparison is descriptive; StockLab does not rank or recommend experiments."}

@app.get("/api/system/capabilities")
def capabilities():
    return {
        "version": APP_VERSION,
        "data": ["provider abstraction", "yfinance", "local CSV", "cache", "dataset fingerprints", "provenance"],
        "research": ["backtest", "OOS", "walk-forward", "robustness", "Monte Carlo", "regime attribution", "ML baseline", "factor diagnostics"],
        "memory": ["SQLite experiments", "theses", "research runs", "portable JSON export"],
        "execution": ["paper trading only"],
        "production_features": ["PostgreSQL", "durable job metadata", "session authentication", "structured LLM tool boundary", "paper broker boundary"],
        "remaining_institutional_work": ["licensed point-in-time data", "object storage", "external queue", "full RBAC", "audited live broker adapter"]
    }

@app.get("/api/system/manifest")
def system_manifest():
    return {
        "platform": "StockLab MAX",
        "version": APP_VERSION,
        "research_layers": ["market-data", "fundamentals", "technical", "screener", "backtest", "discovery", "walk-forward", "robustness", "monte-carlo", "regime", "ml", "portfolio", "research-health", "paper-trading", "experiment-vault", "research-agent", "dossier"],
        "execution": {"paper": True, "broker": False, "live_automation": False},
        "data_provider": DATA_PROVIDER.name,
        "database": DB_INFO.backend,
        "durable_jobs": True,
        "code_fingerprint": code_fingerprint(),
    }


@app.post("/api/research/jobs")
def research_job(payload: dict):
    symbol=str(payload.get("symbol","RELIANCE.NS")); period=str(payload.get("period","5y")); hypothesis=str(payload.get("hypothesis","Investigate the historical behavior of a trend-following strategy."))
    def task():
        return research_run({"symbol":symbol,"period":period,"hypothesis":hypothesis})
    job=submit_job(task, job_type="research", payload={"symbol":symbol,"period":period,"hypothesis":hypothesis})
    return {"job":job,"message":"Research job accepted. Poll /api/jobs/{id} for completion."}

@app.get("/api/health/live")
def health_live():
    return {"status":"ok","service":"stocklab-api","version":APP_VERSION}

@app.get("/api/health/ready")
def health_ready():
    try:
        conn=db(); conn.execute("SELECT 1").fetchone(); conn.close()
        return {"status":"ready","database":DB_INFO.backend,"version":APP_VERSION}
    except Exception as e:
        raise HTTPException(503, f"Not ready: {e}")

@app.get("/api/jobs/{jid}")
def job_status(jid: str):
    job=get_job(jid)
    if not job: raise HTTPException(404,"Job not found.")
    return job

@app.get("/api/jobs")
def jobs(limit:int=50):
    return {"jobs":list_jobs(min(max(limit,1),200))}

def audit_event(event_type, actor="system", details=None):
    try:
        conn=db(); conn.execute("INSERT INTO audit_log(id,ts,actor,event_type,details) VALUES(?,?,?,?,?)", (str(uuid.uuid4())[:12], datetime.now(timezone.utc).isoformat(timespec="seconds"), actor, event_type, json.dumps(details or {}, default=str))); conn.commit(); conn.close()
    except Exception:
        pass

@app.get("/api/audit")
def audit(limit:int=50):
    conn=db(); rows=conn.execute("SELECT id,ts,actor,event_type,details FROM audit_log ORDER BY ts DESC LIMIT ?",(min(max(limit,1),200),)).fetchall(); conn.close()
    return {"events":[{"id":r[0],"ts":r[1],"actor":r[2],"event_type":r[3],"details":json.loads(r[4]) if r[4] else {}} for r in rows]}

@app.post("/api/research-agent/execute")
def research_agent_execute(payload: dict):
    """Execute only whitelisted research tools. No arbitrary code or broker actions."""
    tool=str(payload.get("tool","")); args=payload.get("arguments") or {}
    symbol=str(args.get("symbol","RELIANCE.NS")); period=str(args.get("period","5y"))
    try:
        if tool == "get_quote":
            result=quote(symbol)
        elif tool == "backtest":
            result=bt(data(symbol,period), normalize_params(args.get("params",{})), curve=True)
        elif tool == "regime_attribution":
            x=data(symbol,period); result=regime_strategy_attribution(x, normalize_params(args.get("params",{})))
        elif tool == "research_health":
            result=research_health(Req(symbol=symbol,period=period,**{k:v for k,v in (args.get("params") or {}).items() if k in Req.model_fields}))
        elif tool == "data_health":
            result=data_health(symbol,period)
        else:
            raise HTTPException(400,"Tool is not whitelisted.")
        audit_event("agent_tool", "agent", {"tool":tool,"symbol":symbol,"period":period})
        return {"tool":tool,"arguments":args,"result":result}
    except HTTPException: raise
    except Exception as e: raise HTTPException(400,str(e))

@app.get("/api/research-markdown/{symbol}")
def research_markdown(symbol:str, period:str="5y"):
    d=research_dossier(symbol,period)
    m=d.get("backtest",{})
    rh=d.get("research_health",{})
    lines=[f"# StockLab MAX Research Dossier — {symbol.upper()}","",f"**Period:** {period}",f"**Generated:** {d['generated_at']}","","## Backtest","",f"- Return: {m.get('return')}%",f"- Benchmark: {m.get('benchmark')}%",f"- CAGR: {m.get('cagr')}%",f"- Sharpe: {m.get('sharpe')}",f"- Sortino: {m.get('sortino')}",f"- Max drawdown: {m.get('drawdown')}%",f"- Trades: {m.get('trades')}","","## Research Health","",f"- OOS return gap: {rh.get('oos_return_gap')}",f"- OOS Sharpe gap: {rh.get('oos_sharpe_gap')}","","## Provenance",f"- Provider: {d['provenance'].get('provider')}",f"- Dataset fingerprint: {d['provenance'].get('dataset_fingerprint')}",f"- Code fingerprint: {d['provenance'].get('code_version')}","","## Limitations"]+[f"- {x}" for x in d.get('limitations',[])]
    return {"symbol":symbol.upper(),"period":period,"markdown":"\n".join(lines)}

@app.get("/api/storage/status")
def storage_status():
    return {"active_backend": DB_INFO.backend, "configured_url": DB_INFO.configured, "schema_version": DB_INFO.schema_version, "postgres_enabled": DB_INFO.backend == "postgresql", "note": "SQLite is the zero-config default; PostgreSQL is supported through the same repository boundary when STOCKLAB_DATABASE_URL is configured."}
