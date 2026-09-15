"""Deterministic fault injection, not live-model accuracy or production telemetry."""
import asyncio,json,tempfile
from pathlib import Path
from journal import Journal,UncertainCall
from runner import run_case

RAW={'keywords':[{'name':'해지','turn_id':1}],'sentiment':'NEUTRAL','sentiment_turn':1}
CASE={'id':'operational-fixture','text':'고객: 안 쓰는 회선 해지할게요\n상담사: 네'}
TAX={'해지':['해약']}

class InjectedStop(RuntimeError):pass

class FinalOnlyJournal(Journal):
    """Controlled baseline: only completed job results survive process restart."""
    def __init__(self,path):
        self.final_path=Path(str(path)+'.final.json')
        super().__init__(':memory:')
    def get_job(self,job):
        data=json.loads(self.final_path.read_text()) if self.final_path.exists() else {}
        return data.get(job)
    def complete_job(self,job,result):
        super().complete_job(job,result)
        data=json.loads(self.final_path.read_text()) if self.final_path.exists() else {}
        data[job]=result;self.final_path.write_text(json.dumps(data))

class Model:
    def __init__(self):self.calls=0
    async def __call__(self,payload):
        self.calls+=1
        response={'id':f'fixture-{self.calls}','model':'scripted-fixture','usage':{'prompt_tokens':10,'completion_tokens':10}}
        if 'tools' in payload and not any(m['role']=='tool' for m in payload['messages']):
            message={'role':'assistant','content':None,'tool_calls':[{'id':'lookup-1','type':'function','function':{'name':'lookup_business_keywords','arguments':json.dumps({'query':'해지','limit':2})}}]}
            response['choices']=[{'finish_reason':'tool_calls','message':message}]
        else:response['choices']=[{'finish_reason':'stop','message':{'role':'assistant','content':json.dumps(RAW)}}]
        return response

class Tools:
    identity='operational-fixture-v1'
    def __init__(self):self.calls=0
    async def list_tools(self):
        return [{'name':'lookup_business_keywords','description':'Lookup fixture dictionary','inputSchema':{'type':'object','properties':{'query':{'type':'string'},'limit':{'type':'integer'}},'required':['query','limit'],'additionalProperties':False}}]
    async def call(self,name,args):self.calls+=1;return {'keywords':[{'name':'해지','aliases':['해약']}]}

async def benchmark():
    rows=[]
    for fault in ['normal','after_base_receipt','after_tool_result','before_final_result','response_before_receipt']:
        for mode in ['final_only','step_receipts']:
            base=FinalOnlyJournal if mode=='final_only' else Journal
            state={'injected':False};model=Model();tools=Tools()
            class FaultJournal(base):
                def inject(self):
                    if not state['injected']:
                        state['injected']=True;raise InjectedStop(fault)
                def event(self,job,event,data):
                    super().event(job,event,data)
                    if fault=='after_base_receipt' and event=='MODEL_RESPONSE' and data['step']=='base':self.inject()
                    if fault=='after_tool_result' and event=='TOOL_RESULT':self.inject()
                def finish_call(self,*args,**kwargs):
                    if fault=='response_before_receipt':self.inject()
                    return super().finish_call(*args,**kwargs)
                def complete_job(self,*args,**kwargs):
                    if fault=='before_final_result':self.inject()
                    return super().complete_job(*args,**kwargs)
            with tempfile.TemporaryDirectory() as tmp:
                path=Path(tmp)/'journal.sqlite';result=None
                for attempt in range(2):
                    journal=FaultJournal(path)
                    try:
                        result=await run_case('adaptive',CASE,TAX,model,tools,journal)
                        break
                    except InjectedStop:pass
                    except UncertainCall:
                        result={'status':'STOPPED_UNCERTAIN','prediction':None};break
                    finally:journal.close()
                rows.append({'mode':mode,'fault':fault,'model_calls_actual':model.calls,'tool_calls_actual':tools.calls,'status':result['status'],'prediction':result['prediction'],'fault_injected':state['injected']})
    known=[r for r in rows if r['fault']!='response_before_receipt']
    summary={mode:{'cases':4,'completed':sum(r['status']=='VALIDATED' for r in known if r['mode']==mode),
        'model_calls':sum(r['model_calls_actual'] for r in known if r['mode']==mode),'tool_calls':sum(r['tool_calls_actual'] for r in known if r['mode']==mode)} for mode in ['final_only','step_receipts']}
    return {'kind':'deterministic_injected_fault_test','real_llm_calls':0,'consultation_fixture_count':1,'conditions':5,'rows':rows,'known_receipt_summary':summary,
        'limitations':['Scripted identical model and tools; not semantic accuracy','Final-only baseline models checkpoint placement, not a full historical release benchmark','Unknown in-flight response stops rather than claiming exactly-once API execution','Not a 2000-case run or production outage measurement']}

if __name__=='__main__':
    result=asyncio.run(benchmark())
    path=Path(__file__).parent/'evidence/operational_before_after.json'
    path.write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps(result['known_receipt_summary']))
