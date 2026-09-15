import unittest
from evaluation import summarize,regression_checks

class EvaluationTests(unittest.TestCase):
    def test_http_client_is_part_of_frozen_source_map(self):
        from evaluate import source_map
        self.assertEqual(source_map()['experiment_client.py'].name,'experiment.py')
    def test_regression_constraint_is_not_human_accuracy(self):
        row={'prediction':{'keywords':[{'name':'해지'}],'sentiment':'NEUTRAL'}}
        self.assertTrue(regression_checks(row,{'required':['해지'],'forbidden':['요금제'],'sentiment':'NEUTRAL'})['pass'])
        self.assertFalse(regression_checks(row,{'required':['해지'],'forbidden':[],'sentiment':'NEGATIVE'})['pass'])
    def test_summary_rejects_missing_or_nonidentical_pairs(self):
        row={'id':'1','arm':'single','base_payload_hash':'a'}
        with self.assertRaises(ValueError):summarize([row],[],['1'])
    def test_gate_does_not_pass_on_structural_perfection(self):
        rows=[]
        for arm in ['single','fixed','budget_review','adaptive']:
            rows.append(dict(id='1',arm=arm,base_payload_hash='same',base_raw={'a':1},status='VALIDATED',prediction={'keywords':[],'sentiment':'NEUTRAL'},estimated_usd=.1,uncached_usd=.1,request_seconds=1,elapsed_seconds=1,model_calls=1,tool_calls=0,tool_errors=0,semantic_verified=False,trace=[]))
        result=summarize(rows,[],['1'])
        self.assertFalse(result['usefulness_gate']['passed'])
        self.assertEqual(result['human_gold_verified'],0)
        self.assertEqual(result['arms']['single']['structural_valid'],1)

if __name__=='__main__':unittest.main()
