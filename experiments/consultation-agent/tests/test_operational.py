import unittest
from operational_benchmark import benchmark

class OperationalTests(unittest.IsolatedAsyncioTestCase):
    async def test_faults_compare_identical_work_and_keep_uncertainty_visible(self):
        result=await benchmark()
        rows=result['rows']
        self.assertEqual(len(rows),10)
        by={(r['mode'],r['fault']):r for r in rows}
        for fault in ['normal','after_base_receipt','after_tool_result','before_final_result']:
            before=by[('final_only',fault)];after=by[('step_receipts',fault)]
            self.assertEqual(before['prediction'],after['prediction'])
            self.assertEqual(after['model_calls_actual'],3)
        self.assertEqual(by[('final_only','before_final_result')]['model_calls_actual'],6)
        self.assertEqual(by[('step_receipts','response_before_receipt')]['status'],'STOPPED_UNCERTAIN')
        self.assertEqual(by[('step_receipts','response_before_receipt')]['model_calls_actual'],1)

if __name__=='__main__':unittest.main()
