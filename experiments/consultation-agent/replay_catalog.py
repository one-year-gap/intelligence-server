"""Replay identical initial receipts; change only model-visible tool catalog."""
import argparse,asyncio,hashlib,json,sys,time
from pathlib import Path
import contract as c
from journal import Journal
from mcp_transport import MCPTools
from runner import run_case

H=Path(__file__).resolve().parent;P=H/'inputs';SOURCE=H/'runs/development_v2';OUT=H/'runs/catalog_v3'
def filter_catalog(tools):return [t for t in tools if t['name']!='validate_analysis']
def import_bases(journal,receipts):
    imported=[]
    for r in receipts:
        if r['step']!='base':continue
        if r['status']!='COMPLETE' or not r['response'].get('id'):raise ValueError('unresolved_base')
        journal.begin_call(r['job'],r['step'],r['payload_hash'])
        journal.finish_call(r['job'],r['step'],r['payload_hash'],r['response'],r['cost'],r['seconds'])
        imported.append(r['response']['id'])
    return imported
class ContextTools(MCPTools):
    async def __aenter__(self):
        self.identity=self._backend.identity
        await super().__aenter__()
        self.identity+=':catalog_without_host_validator_v1'
        return self
    async def list_tools(self):return filter_catalog(await super().list_tools())

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def dump(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2))
def prepare():
    if not (SOURCE/'completion.json').exists():raise ValueError('source_incomplete')
    protocol=json.loads((SOURCE/'protocol.json').read_text())
    for name,value in protocol['hashes'].items():
        if sha(SOURCE/name)!=value:raise ValueError('source_snapshot_changed')
    rows=json.loads((SOURCE/'manifest.json').read_text());tax=json.loads((SOURCE/'taxonomy.json').read_text())
    receipts=json.loads((SOURCE/'receipts.json').read_text());bases={r['job']:r for r in receipts if r['step']=='base'}
    if len(rows)!=50 or len(bases)!=50:raise ValueError('expected_exact_development50')
    for r in rows:
        payload=c.base_payload(r['text'],tax,r['id']);key=c.digest({'id':r['id'],'payload':payload})
        if bases[key]['payload_hash']!=c.digest(payload):raise ValueError('base_contract_changed')
    OUT.mkdir(exist_ok=False)
    for name in ['manifest.json','taxonomy.json','receipts.json','results.jsonl']:(OUT/('source_'+name)).write_bytes((SOURCE/name).read_bytes())
    sources=[H/name for name in ['replay_catalog.py','runner.py','journal.py','contract.py','mcp_transport.py','mcp_server.py','tool_backend.py']]+[H/'experiment.py']
    dump(OUT/'protocol.json',{'cases':50,'source':str(SOURCE),'intervention':'Only model catalog removes validate_analysis; full MCP server and common host validation unchanged',
       'initial_responses':'exact imported source receipts, verified payload hash','human_gold':0,
       'limits':'same runner and Limits defaults; source common prompt/model/schema unchanged',
       'development_only':True,'hashes':{str(p):sha(p) for p in sources},
       'source_hashes':{n:sha(OUT/('source_'+n)) for n in ['manifest.json','taxonomy.json','receipts.json','results.jsonl']}})
    print('Prepared catalog-only replay of 50 existing development cases')
async def run():
    if (OUT/'completion.json').exists():print('Already complete; no mutation or API');return
    protocol=json.loads((OUT/'protocol.json').read_text())
    for name,value in protocol['hashes'].items():
        if sha(Path(name))!=value:raise ValueError('frozen_source_changed')
    for name,value in protocol['source_hashes'].items():
        if sha(OUT/('source_'+name))!=value:raise ValueError('frozen_receipts_changed')
    rows=json.loads((OUT/'source_manifest.json').read_text());tax=json.loads((OUT/'source_taxonomy.json').read_text())
    source_receipts=json.loads((OUT/'source_receipts.json').read_text())
    before={r['id']:r for r in map(json.loads,(OUT/'source_results.jsonl').read_text().splitlines()) if r['arm']=='adaptive'}
    journal=Journal(OUT/'state.sqlite')
    imported=import_bases(journal,source_receipts);dump(OUT/'imported_response_ids.json',imported)
    import experiment as e
    api=e.client_from_env()
    async def model(payload):
        for attempt in range(5):
            try:return await asyncio.to_thread(api,payload)
            except e.APIError as ex:
                with (OUT/'retry_events.jsonl').open('a') as log:log.write(json.dumps({'error':str(ex),'attempt':attempt+1})+'\n')
                if 'rate_limit_exceeded' not in str(ex) or attempt==4:raise
                await asyncio.sleep(20)
    results=[]
    try:
        async with ContextTools(OUT/'source_manifest.json',OUT/'source_taxonomy.json') as tools:
            for case in rows:
                r=await run_case('adaptive',case,tax,model,tools,journal)
                if r['base_raw']!=before[case['id']]['base_raw'] or r['base_payload_hash']!=before[case['id']]['base_payload_hash']:raise ValueError('nonidentical_initial_response')
                results.append(r)
                print(json.dumps({'id':r['id'],'calls':r['model_calls'],'tools':r['tool_calls'],'status':r['status']}),flush=True)
        (OUT/'results.jsonl').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in results))
        def metrics(items):return {'structural_valid':sum(r['prediction'] is not None for r in items),'review_required':sum(r['status']=='REVIEW_REQUIRED' for r in items),'model_calls':sum(r['model_calls'] for r in items),'tool_calls':sum(r['tool_calls'] for r in items),'estimated_usd':sum(r['estimated_usd'] for r in items),'uncached_usd':sum(r['uncached_usd'] for r in items)}
        def signature(r):
            pred=r['prediction'] or {};return (tuple(sorted(k['name'] for k in pred.get('keywords',[]))),pred.get('sentiment'))
        unique=[r for r in journal.receipts() if r['status']=='COMPLETE' and r['response']['id'] not in imported]
        summary={'cases':50,'before':metrics(list(before.values())),'after':metrics(results),'labels_changed':sum(signature(r)!=signature(before[r['id']]) for r in results),
            'human_gold':0,'incremental_new_success_calls':len(unique),'incremental_new_estimated_usd':sum(r['cost'] for r in unique),'imported_base_calls':len(imported),
            'same_initial_responses':True,'semantic_quality_change':'unknown','limitations':['Previously used development data','One replay per intervention; review-generation stochasticity not separated','No semantic accuracy or 2000-case gate pass']}
        dump(OUT/'summary.json',summary);dump(OUT/'completion.json',{'cases':50,'status':'complete','incremental_new_estimated_usd':summary['incremental_new_estimated_usd']})
        print(json.dumps(summary),flush=True)
    finally:dump(OUT/'receipts.json',journal.receipts());journal.close()
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['prepare','run']);args=parser.parse_args()
    if args.action=='prepare':prepare()
    else:asyncio.run(run())
