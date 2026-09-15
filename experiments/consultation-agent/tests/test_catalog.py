import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from replay_catalog import filter_catalog,import_bases
from journal import Journal

class CatalogTests(unittest.TestCase):
    def test_only_redundant_host_validator_removed(self):
        tools=[{'name':n} for n in ['get_consultation_turns','validate_analysis','lookup_business_keywords']]
        self.assertEqual([t['name'] for t in filter_catalog(tools)],['get_consultation_turns','lookup_business_keywords'])
        self.assertEqual(len(tools),3)
    def test_imported_receipts_keep_original_usage_and_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            j=Journal(Path(tmp)/'j.sqlite')
            r={'job':'b','step':'base','payload_hash':'h','status':'COMPLETE','response':{'id':'original','usage':{'prompt_tokens':20}},'cost':.1,'seconds':2}
            imported=import_bases(j,[r])
            self.assertEqual(imported,['original'])
            self.assertEqual(j.begin_call('b','base','h')['response'],r['response'])
            j.close()

class CompletedRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_run_returns_without_key_or_mutation(self):
        import evaluate
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);(p/'completion.json').write_text('{"cases":1}')
            before=(p/'completion.json').read_bytes()
            with patch.object(evaluate,'folder',return_value=p):await evaluate.run('completed')
            self.assertEqual((p/'completion.json').read_bytes(),before)
            self.assertEqual(len(list(p.iterdir())),1)
