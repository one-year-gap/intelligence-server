"""Durable receipts; unknown in-flight model requests never silently retried."""
import json
import sqlite3
import time

class UncertainCall(RuntimeError):pass

class Journal:
    def __init__(self,path):
        self.db=sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS calls(job TEXT, step TEXT, payload_hash TEXT NOT NULL, status TEXT NOT NULL,
          response TEXT, cost REAL, seconds REAL, PRIMARY KEY(job,step));
        CREATE TABLE IF NOT EXISTS jobs(job TEXT PRIMARY KEY,result TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,job TEXT,event TEXT,data TEXT,created REAL);
        ''')
    def close(self):self.db.close()
    def begin_call(self,job,step,payload_hash):
        with self.db:
            row=self.db.execute('SELECT payload_hash,status,response,cost,seconds FROM calls WHERE job=? AND step=?',(job,step)).fetchone()
            if row:
                if row[0]!=payload_hash:raise ValueError('payload_changed_for_existing_step')
                if row[1]!='COMPLETE':raise UncertainCall(f'Unresolved in-flight request: {job}/{step}')
                return {'response':json.loads(row[2]),'cost':row[3],'seconds':row[4]}
            self.db.execute('INSERT INTO calls(job,step,payload_hash,status) VALUES(?,?,?,?)',(job,step,payload_hash,'PENDING'))
        return None
    def finish_call(self,job,step,payload_hash,response,cost,seconds=0):
        encoded=json.dumps(response,ensure_ascii=False,sort_keys=True)
        with self.db:
            row=self.db.execute('SELECT payload_hash,status,response,cost FROM calls WHERE job=? AND step=?',(job,step)).fetchone()
            if not row or row[0]!=payload_hash:raise ValueError('unknown_or_mismatched_call')
            if row[1]=='COMPLETE':
                if row[2]!=encoded or row[3]!=cost:raise ValueError('completed_response_changed')
                return
            self.db.execute('UPDATE calls SET status=?,response=?,cost=?,seconds=? WHERE job=? AND step=?',('COMPLETE',encoded,cost,seconds,job,step))
    def total_cost(self):return self.db.execute('SELECT COALESCE(SUM(cost),0) FROM calls WHERE status=?',('COMPLETE',)).fetchone()[0]
    def get_call(self,job,step):
        row=self.db.execute('SELECT payload_hash,status FROM calls WHERE job=? AND step=?',(job,step)).fetchone()
        return {'payload_hash':row[0],'status':row[1]} if row else None
    def get_job(self,job):
        row=self.db.execute('SELECT result FROM jobs WHERE job=?',(job,)).fetchone()
        return json.loads(row[0]) if row else None
    def complete_job(self,job,result):
        encoded=json.dumps(result,ensure_ascii=False,sort_keys=True)
        with self.db:
            old=self.db.execute('SELECT result FROM jobs WHERE job=?',(job,)).fetchone()
            if old:
                if old[0]!=encoded:raise ValueError('completed_job_changed')
                return
            self.db.execute('INSERT INTO jobs VALUES(?,?)',(job,encoded))
    def event(self,job,event,data):
        with self.db:self.db.execute('INSERT INTO events(job,event,data,created) VALUES(?,?,?,?)',(job,event,json.dumps(data,ensure_ascii=False),time.time()))
    def events(self,job):
        return [{'event':r[0],'data':json.loads(r[1]),'created':r[2]} for r in self.db.execute('SELECT event,data,created FROM events WHERE job=? ORDER BY id',(job,))]
    def receipts(self):
        return [{'job':r[0],'step':r[1],'payload_hash':r[2],'status':r[3],'response':json.loads(r[4]) if r[4] else None,'cost':r[5],'seconds':r[6]} for r in self.db.execute('SELECT job,step,payload_hash,status,response,cost,seconds FROM calls ORDER BY rowid')]
