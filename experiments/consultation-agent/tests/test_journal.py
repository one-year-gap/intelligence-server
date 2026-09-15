import tempfile
import unittest
from pathlib import Path
from journal import Journal,UncertainCall

class JournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'state.sqlite'
        self.j=Journal(self.path)
    def tearDown(self):self.j.close();self.tmp.cleanup()
    def test_response_is_reused_across_restart(self):
        self.assertIsNone(self.j.begin_call('job','base','hash'))
        response={'choices':[{'message':{'content':'{}'}}],'usage':{'prompt_tokens':1}}
        self.j.finish_call('job','base','hash',response,.5)
        self.j.close();self.j=Journal(self.path)
        self.assertEqual(self.j.begin_call('job','base','hash')['response'],response)
        self.assertEqual(self.j.total_cost(),.5)
    def test_uncertain_call_is_not_silently_repeated(self):
        self.j.begin_call('job','base','hash')
        self.j.close();self.j=Journal(self.path)
        with self.assertRaises(UncertainCall):self.j.begin_call('job','base','hash')
    def test_same_step_different_payload_rejected(self):
        self.j.begin_call('job','base','hash')
        with self.assertRaises(ValueError):self.j.begin_call('job','base','different')
    def test_finish_is_idempotent_not_double_charged(self):
        self.j.begin_call('job','base','hash');r={'response':1}
        self.j.finish_call('job','base','hash',r,.2)
        self.j.finish_call('job','base','hash',r,.2)
        self.assertAlmostEqual(self.j.total_cost(),.2)
    def test_job_result_is_immutable_and_events_are_append_only(self):
        self.j.event('j','DRAFTED',{'n':1});self.j.event('j','CHECKED',{'n':2})
        self.j.complete_job('j',{'status':'VALIDATED'})
        self.assertEqual(self.j.get_job('j'),{'status':'VALIDATED'})
        with self.assertRaises(ValueError):self.j.complete_job('j',{'status':'DIFFERENT'})
        self.assertEqual([r['event'] for r in self.j.events('j')],['DRAFTED','CHECKED'])

if __name__=='__main__':unittest.main()
