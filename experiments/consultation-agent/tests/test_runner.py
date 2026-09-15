import copy,json,tempfile,unittest
from pathlib import Path
import contract as c
from journal import Journal
from runner import run_case,Limits

CASE={'id':'case-1','text':'고객: 해지할게요\n상담사: 네'}
TAX={'해지':['해약']}
RAW={'keywords':[{'name':'해지','turn_id':1}],'sentiment':'NEUTRAL','sentiment_turn':1}
def completion(raw):
    return {'id':'fake','model':c.MODEL,'usage':{'prompt_tokens':10,'completion_tokens':10},'choices':[{'finish_reason':'stop','message':{'role':'assistant','content':json.dumps(raw)}}]}
def tool_request():
    return {'id':'fake-tool','model':c.MODEL,'usage':{'prompt_tokens':10,'completion_tokens':10},'choices':[{'finish_reason':'tool_calls','message':{'role':'assistant','content':None,'tool_calls':[{'id':'call-1','type':'function','function':{'name':'lookup_business_keywords','arguments':json.dumps({'query':'해지','limit':2})}}]}}]}
class Model:
    def __init__(self,responses):self.responses=iter(responses);self.payloads=[]
    async def __call__(self,payload):self.payloads.append(copy.deepcopy(payload));return next(self.responses)
class Tools:
    identity='fixture-v1'
    def __init__(self,fail=False):self.calls=[];self.fail=fail
    async def list_tools(self):return [{'name':'lookup_business_keywords','description':'lookup','inputSchema':{'type':'object','properties':{'query':{'type':'string'},'limit':{'type':'integer'}},'required':['query','limit'],'additionalProperties':False}}]
    async def call(self,name,args):
        self.calls.append((name,args))
        if self.fail:raise ValueError('lookup_unavailable')
        return {'matches':[{'name':'해지','aliases':['해약']}]}

class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.j=Journal(Path(self.tmp.name)/'state.sqlite');self.tools=Tools()
    async def asyncTearDown(self):self.j.close();self.tmp.cleanup()
    async def test_shared_initial_response_and_schema_across_arms(self):
        model=Model([completion(RAW),completion(RAW),completion(RAW)])
        results=[await run_case(a,CASE,TAX,model,self.tools,self.j) for a in ['single','fixed','adaptive']]
        self.assertEqual(len(model.payloads),3)
        self.assertEqual(model.payloads[0],c.base_payload(CASE['text'],TAX,CASE['id']))
        for payload in model.payloads:
            self.assertEqual(payload['response_format'],model.payloads[0]['response_format'])
            self.assertEqual(payload['messages'][:2],model.payloads[0]['messages'])
        self.assertTrue(all(r['base_raw']==RAW for r in results))
        self.assertEqual(model.payloads[-1]['tool_choice'],'auto')
        self.assertFalse(any(r['semantic_verified'] for r in results))
    async def test_adaptive_uses_actual_tool_result_then_finishes(self):
        model=Model([completion(RAW),tool_request(),completion(RAW)])
        result=await run_case('adaptive',CASE,TAX,model,self.tools,self.j)
        self.assertEqual(result['tool_calls'],1)
        self.assertEqual(result['status'],'VALIDATED')
        self.assertEqual(len(self.tools.calls),1)
        self.assertTrue(any(m['role']=='tool' for m in model.payloads[-1]['messages']))
    async def test_repeated_identical_tool_call_stops_without_repeat(self):
        model=Model([completion(RAW),tool_request(),tool_request()])
        result=await run_case('adaptive',CASE,TAX,model,self.tools,self.j)
        self.assertEqual(result['status'],'REVIEW_REQUIRED')
        self.assertEqual(result['stop_reason'],'repeated_tool_without_progress')
        self.assertEqual(len(self.tools.calls),1)
    async def test_completed_job_resume_does_not_call_model(self):
        model=Model([completion(RAW)])
        first=await run_case('single',CASE,TAX,model,self.tools,self.j)
        again=await run_case('single',CASE,TAX,model,self.tools,self.j)
        self.assertEqual(len(model.payloads),1)
        self.assertTrue(again['restored'])
        self.assertEqual(first['raw'],again['raw'])
    async def test_budget_stops_before_api(self):
        model=Model([])
        result=await run_case('single',CASE,TAX,model,self.tools,self.j,Limits(run_budget_usd=0))
        self.assertEqual(len(model.payloads),0)
        self.assertEqual(result['stop_reason'],'budget_limit')
    async def test_tool_failure_is_visible_not_success(self):
        model=Model([completion(RAW),tool_request(),completion(RAW)])
        result=await run_case('adaptive',CASE,TAX,model,Tools(fail=True),self.j)
        self.assertEqual(result['tool_errors'],1)
        self.assertTrue(any('lookup_unavailable' in json.dumps(x) for x in result['trace']))
    async def test_max_model_calls_leaves_unresolved(self):
        model=Model([completion(RAW),tool_request()])
        result=await run_case('adaptive',CASE,TAX,model,self.tools,self.j,Limits(max_model_calls=2))
        self.assertEqual(result['stop_reason'],'model_call_limit')
        self.assertEqual(result['status'],'REVIEW_REQUIRED')
    async def test_discovery_failure_returns_unresolved(self):
        class Broken(Tools):
            async def list_tools(self):raise ValueError('discovery_unavailable')
        result=await run_case('adaptive',CASE,TAX,Model([completion(RAW)]),Broken(),self.j)
        self.assertEqual(result['status'],'REVIEW_REQUIRED')
        self.assertEqual(result['stop_reason'],'tool_discovery_failed')
    async def test_failed_tool_output_replayed_after_crash(self):
        class CrashJournal(Journal):
            crash=True
            def event(self,job,event,data):
                super().event(job,event,data)
                if event=='TOOL_ERROR' and self.crash:
                    self.crash=False;raise RuntimeError('injected_process_stop')
        self.j.close();self.j=CrashJournal(Path(self.tmp.name)/'state.sqlite')
        tools=Tools(fail=True);model=Model([completion(RAW),tool_request(),completion(RAW)])
        with self.assertRaisesRegex(RuntimeError,'injected_process_stop'):
            await run_case('adaptive',CASE,TAX,model,tools,self.j)
        tools.fail=False
        result=await run_case('adaptive',CASE,TAX,model,tools,self.j)
        self.assertEqual(len(tools.calls),1)
        self.assertEqual(result['tool_errors'],1)
        self.assertEqual(result['stop_reason'],'tool_failure_unresolved')
    async def test_malformed_provider_envelope_is_recorded(self):
        for response in [None,{'choices':[]},{'choices':[{'finish_reason':'stop','message':None}]}]:
            with tempfile.TemporaryDirectory() as tmp:
                j=Journal(Path(tmp)/'state.sqlite')
                try:
                    result=await run_case('single',CASE,TAX,Model([response]),self.tools,j)
                    self.assertEqual(result['stop_reason'],'invalid_response')
                finally:j.close()
    async def test_nonlist_tool_calls_is_recorded(self):
        response=tool_request();response['choices'][0]['message']['tool_calls']='invalid'
        result=await run_case('adaptive',CASE,TAX,Model([completion(RAW),response]),self.tools,self.j)
        self.assertEqual(result['stop_reason'],'invalid_response')
    async def test_malformed_tool_arguments_do_not_crash(self):
        malformed=tool_request();malformed['choices'][0]['message']['tool_calls'][0]['function']['arguments']='{bad'
        result=await run_case('adaptive',CASE,TAX,Model([completion(RAW),malformed,completion(RAW)]),self.tools,self.j)
        self.assertEqual(result['tool_errors'],1)
        self.assertEqual(result['status'],'REVIEW_REQUIRED')
    async def test_budget_review_uses_same_call_ceiling_and_output_contract(self):
        model=Model([completion(RAW)]*4)
        result=await run_case('budget_review',CASE,TAX,model,self.tools,self.j,Limits(max_model_calls=4))
        self.assertEqual(result['model_calls'],4)
        self.assertTrue(all(p['response_format']==model.payloads[0]['response_format'] for p in model.payloads))
        self.assertTrue(all('tools' not in p for p in model.payloads))

if __name__=='__main__':unittest.main()
