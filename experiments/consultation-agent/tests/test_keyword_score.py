import unittest
import keyword_score as k

class ScoreTests(unittest.TestCase):
    def test_counts_false_positives_and_missing_output(self):
        gold={'x':['A','B'],'y':['C']}
        rows=[{'id':'x','prediction':{'keywords':[{'name':'A'},{'name':'D'}]},'status':'VALIDATED'}, {'id':'y','prediction':None,'status':'REVIEW_REQUIRED'}]
        s=k.score(rows,gold)
        self.assertEqual((s['tp'],s['fp'],s['fn']),(1,1,2))
        self.assertAlmostEqual(s['micro_f1'],0.4)
        self.assertEqual(s['exact_matches'],0)
        self.assertEqual(s['cases'],2)
    def test_abstention_not_counted_as_exact_even_empty_gold(self):
        s=k.score([{'id':'x','prediction':{'keywords':[]},'status':'REVIEW_REQUIRED'}],{'x':[]})
        self.assertEqual(s['exact_matches'],0)
        self.assertEqual(s['review_required'],1)
    def test_rejects_duplicate_or_missing_cases(self):
        r={'id':'x','prediction':{'keywords':[]},'status':'VALIDATED'}
        for rows in [[r,r],[]]:
            with self.assertRaises(ValueError):k.score(rows,{'x':[]})
    def test_deduplicates_labels(self):
        s=k.score([{'id':'x','prediction':{'keywords':[{'name':'A'},{'name':'A'}]},'status':'VALIDATED'}],{'x':['A']})
        self.assertEqual(s['tp'],1)
        self.assertEqual(s['micro_f1'],1)
        self.assertEqual(s['exact_matches'],1)
