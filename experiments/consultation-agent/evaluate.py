"""Prepare frozen development runs and execute real shared-contract comparisons."""
import argparse,asyncio,hashlib,importlib.metadata,json,re,sys,time
from pathlib import Path
import contract as c
from evaluation import ARMS,summarize
from journal import Journal
from mcp_transport import MCPTools
from runner import Limits,run_case

HERE=Path(__file__).resolve().parent;P=HERE/'inputs'
SOURCES=['contract.py','journal.py','runner.py','evaluation.py','evaluate.py','tool_backend.py','mcp_server.py','mcp_transport.py']
def source_map():return {**{name:HERE/name for name in SOURCES},'experiment_client.py':HERE/'experiment.py'}
def sha(data):return hashlib.sha256(data).hexdigest()
def dump(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2))
def folder(name):
    if not re.fullmatch('[a-z0-9_-]{1,60}',name):raise ValueError('invalid_run_name')
    return HERE/'runs'/name
def prepare(name,dataset):
    out=folder(name);out.mkdir(parents=True,exist_ok=False)
    expected=None;rows=[]
    if dataset=='regression':
        source=json.loads((P/'semantic_regressions.json').read_text())
        rows=[{'id':r['id'],'text':r['text']} for r in source]
        expected={r['id']:{k:r[k] for k in ['required','forbidden','sentiment']} for r in source}
    elif dataset=='development':
        root=Path(json.loads((P/'dataset_summary.json').read_text())['source_root'])
        for line in (P/'development_50.jsonl').read_text().splitlines():
            r=json.loads(line);data=(root/r['path']).read_bytes()
            if sha(data)!=r['source_file_sha256']:raise ValueError('source_hash_changed')
            text=json.loads(data)[r['record_index']]['consulting_content']
            text=re.sub(r'[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}','<EMAIL>',text)
            text=re.sub(r'(?<!\d)(?:01[016789]|0[2-6][1-5]?)[ -]?\d{3,4}[ -]?\d{4}(?!\d)','<PHONE_NUMBER>',text)
            rows.append({'id':r['id'],'text':text,'source_file_sha256':r['source_file_sha256']})
    else:raise ValueError('Only pre-existing development runs allowed; 2000 gated separately')
    for r in rows:r['input_sha256']=sha(r['text'].encode())
    dump(out/'manifest.json',rows)
    (out/'taxonomy.json').write_bytes((P/'business_keyword_alias_map.snapshot.json').read_bytes())
    if expected:dump(out/'expected_development_constraints.json',expected)
    for name,path in source_map().items():(out/name).write_bytes(path.read_bytes())
    files=list(source_map())+['manifest.json','taxonomy.json']+(['expected_development_constraints.json'] if expected else [])
    dump(out/'protocol.json',{'dataset':dataset,'cases':len(rows),'model':c.MODEL,'arms':ARMS,'model_call_ceiling':4,'tool_call_ceiling':6,'run_budget_usd':2.0,
        'human_gold':0,'initial_response':'shared','arm_order':'cyclic latin rotation','information':'all complete turns and full dictionary preloaded identically',
        'constraints':'same strict customer-turn-ID schema, same reconstruction/dedup/validation',
        'sdk':importlib.metadata.version('mcp'),'hashes':{name:sha((out/name).read_bytes()) for name in files},
        'scope':'development debug only, not held-out semantic accuracy or 2000-case evaluation'})
    print(json.dumps({'prepared':str(out),'cases':len(rows)}))
async def run(name):
    out=folder(name)
    if (out/'completion.json').exists():
        print('Already complete; no mutation or API');return
    protocol=json.loads((out/'protocol.json').read_text())
    if protocol['cases']>50:raise ValueError('Large evaluation gate not passed')
    for name,h in protocol['hashes'].items():
        if sha((out/name).read_bytes())!=h:raise ValueError('snapshot_changed')
        if name in source_map() and sha(source_map()[name].read_bytes())!=h:raise ValueError('source_changed_after_freeze')
    if importlib.metadata.version('mcp')!=protocol['sdk']:raise ValueError('sdk_changed')
    import experiment as e
    api=e.client_from_env()
    retrylog=(out/'retry_events.jsonl').open('a')
    async def model(payload):
        for attempt in range(5):
            try:return await asyncio.to_thread(api,payload)
            except e.APIError as ex:
                retrylog.write(json.dumps({'code':str(ex),'attempt':attempt+1,'payload_hash':c.digest(payload)})+'\n');retrylog.flush()
                if 'rate_limit_exceeded' not in str(ex) or attempt==4:raise
                await asyncio.sleep(20)
    journal=Journal(out/'state.sqlite');rows=json.loads((out/'manifest.json').read_text());tax=json.loads((out/'taxonomy.json').read_text())
    results=[];started=time.perf_counter();startup=time.perf_counter()
    try:
        async with MCPTools(out/'manifest.json',out/'taxonomy.json') as tools:
            dump(out/'transport_startup.json',{'seconds':time.perf_counter()-startup,'sdk':protocol['sdk']})
            for index,row in enumerate(rows):
                order=ARMS[index%4:]+ARMS[:index%4]
                for arm in order:
                    result=await run_case(arm,row,tax,model,tools,journal,Limits())
                    results.append(result)
                    dump(out/'progress.json',{'completed_arm_results':len(results),'cases_total':len(rows),'actual_receipt_estimated_usd':journal.total_cost(),'elapsed_seconds':time.perf_counter()-started})
                    print(json.dumps({'id':row['id'],'arm':arm,'status':result['status'],'model_calls':result['model_calls'],'tool_calls':result['tool_calls'],'actual_usd':round(journal.total_cost(),8)}),flush=True)
        (out/'results.jsonl').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in results))
        receipts=journal.receipts();dump(out/'receipts.json',receipts)
        # Gold-like development expectations are loaded ONLY after all model execution.
        path=out/'expected_development_constraints.json';expected=json.loads(path.read_text()) if path.exists() else None
        summary=summarize(results,receipts,[r['id'] for r in rows],expected)
        dump(out/'summary.json',summary);dump(out/'completion.json',{'cases':len(rows),'arm_results':len(results),'actual_usd':journal.total_cost(),'elapsed_seconds':time.perf_counter()-started})
        print(json.dumps(summary,ensure_ascii=False),flush=True)
    finally:
        dump(out/'receipts.json',journal.receipts());journal.close();retrylog.close()
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['prepare','run']);parser.add_argument('--name',required=True);parser.add_argument('--dataset',choices=['regression','development'])
    args=parser.parse_args()
    if args.action=='prepare':prepare(args.name,args.dataset)
    else:asyncio.run(run(args.name))
