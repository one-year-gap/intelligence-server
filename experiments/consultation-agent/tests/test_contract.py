import copy
import unittest
import contract as c

TEXT='상담사: 요금제를 올려보세요\n고객: 아니요. 비밀번호 찾기만 해주세요\n고객: 불편은 없어요'
TAX={'요금제':['플랜'],'비밀번호 찾기':['비밀번호']}
RAW={'keywords':[{'name':'비밀번호 찾기','turn_id':2}],'sentiment':'NEUTRAL','sentiment_turn':3}

class ContractTests(unittest.TestCase):
    def test_complete_input_and_no_metadata_in_payload(self):
        p=c.base_payload(TEXT,TAX)
        import json
        data=json.loads(p['messages'][1]['content'])
        self.assertEqual(data['dictionary'],TAX)
        self.assertEqual(data['turns'],c.turns(TEXT))
        self.assertEqual(set(data),{'dictionary','turns'})
        self.assertEqual(p['response_format'],c.schema(TAX,TEXT))
    def test_quote_is_reconstructed_without_semantic_claim(self):
        p=c.resolve(RAW,TEXT,TAX)
        self.assertEqual(p['keywords'][0]['evidence'],'아니요. 비밀번호 찾기만 해주세요')
        self.assertEqual(p['sentiment_evidence'],'불편은 없어요')
    def test_rejects_advisor_or_unknown_reference(self):
        for bad in [1,99,True]:
            raw=copy.deepcopy(RAW);raw['sentiment_turn']=bad
            with self.assertRaises(ValueError):c.resolve(raw,TEXT,TAX)
    def test_rejects_unknown_labels_and_extra_fields(self):
        raw=copy.deepcopy(RAW);raw['keywords'][0]['name']='없는 분류'
        with self.assertRaises(ValueError):c.resolve(raw,TEXT,TAX)
        with self.assertRaises(ValueError):c.resolve({**RAW,'gold':'yes'},TEXT,TAX)
    def test_dedup_does_not_mutate_and_preserves_first_reference(self):
        raw=copy.deepcopy(RAW);raw['keywords']*=2
        self.assertEqual(len(c.resolve(raw,TEXT,TAX)['keywords']),1)
        self.assertEqual(len(raw['keywords']),2)
    def test_no_customer_cannot_be_fabricated(self):
        with self.assertRaises(ValueError):c.base_payload('상담사: 안녕하세요',TAX)
    def test_unknown_lines_remain_in_context(self):
        rows=c.turns('고객: 첫째\n이어서 한 말\n상담사: 네')
        self.assertEqual(len(rows),3)
        self.assertEqual(rows[1]['speaker'],'UNKNOWN')
    def test_risk_is_not_an_accuracy_label(self):
        self.assertEqual(c.risk_signals(RAW,TEXT,TAX),[])
        raw=copy.deepcopy(RAW);raw['sentiment']='AMBIGUOUS'
        self.assertIn('ambiguous_sentiment',c.risk_signals(raw,TEXT,TAX))

if __name__=='__main__':unittest.main()
