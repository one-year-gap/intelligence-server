import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import asyncio
from unittest.mock import patch
from contextlib import asynccontextmanager


class Fixture:
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.manifest = Path(self.tmp.name) / 'manifest.json'
        self.taxonomy = Path(self.tmp.name) / 'taxonomy.json'
        self.text = '상담사: 안녕하세요\n고객: 요금이 비싸요\n상담사: 확인하겠습니다\n고객: 감사합니다'
        self.sha = hashlib.sha256(self.text.encode()).hexdigest()
        self.manifest.write_text(json.dumps([{'id': 'tiny-1', 'text': self.text, 'input_sha256': self.sha}]), encoding='utf-8')
        self.taxonomy.write_text(json.dumps({'요금': ['가격', '요금']}), encoding='utf-8')

    def backend(self):
        self.assertIsNotNone(importlib.util.find_spec('tool_backend'), 'ToolBackend implementation missing')
        from tool_backend import ToolBackend
        return ToolBackend(self.manifest, self.taxonomy)


class BackendTests(Fixture, unittest.TestCase):
    def test_assert_case_rejects_mismatched_text_and_taxonomy(self):
        from mcp_transport import MCPTools
        backend = self.backend()
        self.assertTrue(hasattr(backend, 'assert_case'), 'case snapshot check missing')
        case = {'id': 'tiny-1', 'text': self.text}
        taxonomy = json.loads(self.taxonomy.read_text())
        for target in (backend, MCPTools(self.manifest, self.taxonomy)):
            target.assert_case(case, taxonomy)
            with self.assertRaises(ValueError):
                target.assert_case({**case, 'text': self.text + '추가'}, taxonomy)
            with self.assertRaises(ValueError):
                target.assert_case(case, {'요금': ['다른 별칭']})

    def test_identity_tracks_snapshot_and_source_versions(self):
        backend = self.backend()
        self.assertTrue(hasattr(backend, 'identity'), 'versioned tool identity missing')
        from tool_backend import ToolBackend
        from mcp_transport import MCPTools
        self.assertEqual(backend.identity, MCPTools(self.manifest, self.taxonomy).identity)
        self.assertEqual(backend.identity, ToolBackend(self.manifest, self.taxonomy).identity)
        self.taxonomy.write_text(json.dumps({'요금': ['가격', '요금', '비용']}))
        self.assertNotEqual(backend.identity, ToolBackend(self.manifest, self.taxonomy).identity)
        original = Path.read_bytes
        before_source_change = ToolBackend(self.manifest, self.taxonomy).identity
        def changed_source(path):
            data = original(path)
            return data + b'\n# simulated source revision\n' if path.name == 'contract.py' else data
        with patch.object(Path, 'read_bytes', changed_source):
            self.assertNotEqual(before_source_change, ToolBackend(self.manifest, self.taxonomy).identity)
        self.manifest.write_text(self.manifest.read_text() + '\n')
        self.assertNotEqual(before_source_change, ToolBackend(self.manifest, self.taxonomy).identity)

    def test_scoped_turns_and_hash(self):
        from contract import turns
        backend = self.backend()
        tid = turns(self.text)[1]['id']
        result = backend.dispatch('get_consultation_turns', {'consultation_id': 'tiny-1', 'turn_ids': [tid], 'window': 1})
        self.assertEqual(result['turns'], turns(self.text)[:3])
        self.assertEqual(result['input_sha256'], self.sha)
        with self.assertRaises(ValueError):
            backend.dispatch('get_consultation_turns', {'consultation_id': '../secret', 'turn_ids': [tid], 'window': 0})

    def test_keyword_alias_version_and_bounds(self):
        backend = self.backend()
        result = backend.dispatch('lookup_business_keywords', {'query': '가격', 'limit': 1})
        self.assertEqual(result['keywords'], [{'name': '요금', 'aliases': ['가격', '요금']}])
        self.assertEqual(result['taxonomy_version'], hashlib.sha256(self.taxonomy.read_bytes()).hexdigest())
        for args in [{'query': '가격', 'limit': 0}, {'query': '가격', 'limit': 21}, {'query': '가격', 'limit': True}, {'query': '가격', 'limit': 1, 'path': '/tmp'}]:
            with self.assertRaises(ValueError):
                backend.dispatch('lookup_business_keywords', args)

    def test_structural_validation_reconstructs_customer_evidence(self):
        from contract import turns
        backend = self.backend()
        ts = turns(self.text)
        raw = {'keywords': [{'name': '요금', 'turn_id': ts[1]['id']}], 'sentiment': 'NEGATIVE', 'sentiment_turn': ts[1]['id']}
        result = backend.dispatch('validate_analysis', {'consultation_id': 'tiny-1', 'analysis_json': json.dumps(raw)})
        self.assertTrue(result['valid'], result)
        self.assertFalse(result['semantic_verified'])
        self.assertEqual(result['resolved']['keywords'][0]['evidence'], ts[1]['text'])
        raw['keywords'][0]['turn_id'] = ts[0]['id']
        result = backend.dispatch('validate_analysis', {'consultation_id': 'tiny-1', 'analysis_json': json.dumps(raw)})
        self.assertFalse(result['valid'])
        self.assertTrue(result['errors'])

    def test_bad_manifest_hash_rejected(self):
        self.backend()
        from tool_backend import ToolBackend
        rows = json.loads(self.manifest.read_text())
        rows[0]['input_sha256'] = 'wrong'
        self.manifest.write_text(json.dumps(rows))
        with self.assertRaises(ValueError):
            ToolBackend(self.manifest, self.taxonomy)

    def test_single_record_jsonl_and_turn_bounds(self):
        self.backend()
        from tool_backend import ToolBackend
        manifest = self.manifest.with_suffix('.jsonl')
        manifest.write_text(json.dumps(json.loads(self.manifest.read_text())[0]) + '\n')
        backend = ToolBackend(manifest, self.taxonomy)
        for ids, window in [([], 0), ([True], 0), ([999], 0), ([1], -1), ([1], 4)]:
            with self.assertRaises(ValueError):
                backend.dispatch('get_consultation_turns', {'consultation_id': 'tiny-1', 'turn_ids': ids, 'window': window})


class MCPTests(Fixture, unittest.IsolatedAsyncioTestCase):
    async def test_transport_errors_sanitized_and_cancellation_preserved(self):
        from mcp_transport import MCPTools
        class BrokenSession:
            async def list_tools(self):
                raise RuntimeError('Authorization: SECRET')
            async def call_tool(self, *args, **kwargs):
                raise ConnectionError('Authorization: SECRET')
        client = MCPTools(self.manifest, self.taxonomy)
        client._session = BrokenSession()
        for operation in [client.list_tools(), client.call('irrelevant', {})]:
            with self.assertRaises(ValueError) as caught:
                await operation
            self.assertNotIn('SECRET', str(caught.exception))
        class CancelledSession:
            async def list_tools(self):
                raise asyncio.CancelledError()
        client._session = CancelledSession()
        with self.assertRaises(asyncio.CancelledError):
            await client.list_tools()

    async def test_discovery_timeout_and_context_cleanup(self):
        from mcp_transport import MCPTools
        class HangingSession:
            async def list_tools(self):
                await asyncio.Event().wait()
        client = MCPTools(self.manifest, self.taxonomy)
        client.timeout_seconds = 0.02
        client._session = HangingSession()
        with self.assertRaises(ValueError):
            await asyncio.wait_for(client.list_tools(), 0.2)
        actual = MCPTools(self.manifest, self.taxonomy)
        with self.assertRaisesRegex(RuntimeError, 'caller error'):
            async with actual:
                raise RuntimeError('caller error')
        self.assertIsNone(actual._session)
        self.assertIsNone(actual._stack)

    async def test_startup_timeout_closes_resources(self):
        from mcp_transport import MCPTools
        events = []
        @asynccontextmanager
        async def transport(params):
            try:
                events.append('opened')
                yield (object(), object())
            finally:
                events.append('closed')
        class HangingStartup:
            def __init__(self, *args):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                events.append('session closed')
            async def initialize(self):
                await asyncio.Event().wait()
        client = MCPTools(self.manifest, self.taxonomy)
        client.timeout_seconds = 0.02
        with patch('mcp_transport.stdio_client', transport), patch('mcp_transport.ClientSession', HangingStartup):
            with self.assertRaisesRegex(ValueError, 'MCP startup failed'):
                await asyncio.wait_for(client.__aenter__(), 0.2)
        self.assertEqual(events, ['opened', 'session closed', 'closed'])
        self.assertIsNone(client._session)
        self.assertIsNone(client._stack)

    async def test_cleanup_error_does_not_convert_cancellation_to_value_error(self):
        from mcp_transport import MCPTools
        class BrokenStack:
            async def aclose(self):
                raise RuntimeError('Authorization: SECRET')
        client = MCPTools(self.manifest, self.taxonomy)
        client._stack = BrokenStack()
        cancelled = asyncio.CancelledError()
        try:
            await client.__aexit__(asyncio.CancelledError, cancelled, None)
        except asyncio.CancelledError:
            pass
        except ValueError:
            self.fail('cleanup converted cancellation into a catchable tool error')

    async def test_real_stdio_discovery_and_call_parity(self):
        backend = self.backend()
        self.assertIsNotNone(importlib.util.find_spec('mcp_transport'), 'MCP transport implementation missing')
        from mcp_transport import MCPTools
        from contract import turns
        ts = turns(self.text)
        calls = [('get_consultation_turns', {'consultation_id': 'tiny-1', 'turn_ids': [ts[1]['id']], 'window': 1}),
                 ('lookup_business_keywords', {'query': '가격', 'limit': 2}),
                 ('validate_analysis', {'consultation_id': 'tiny-1', 'analysis_json': json.dumps({'keywords': [], 'sentiment': 'NEGATIVE', 'sentiment_turn': ts[1]['id']})}),
                 ('validate_analysis', {'consultation_id': 'tiny-1', 'analysis_json': '{invalid'})]
        async with MCPTools(self.manifest, self.taxonomy) as client:
            self.assertEqual(await client.list_tools(), backend.list_tools())
            for name, args in calls:
                self.assertEqual(await client.call(name, args), backend.dispatch(name, args))
            with self.assertRaises(ValueError):
                await client.call('get_consultation_turns', {'consultation_id': 'missing', 'turn_ids': [1], 'window': 0})
            with self.assertRaises(ValueError):
                await client.call('lookup_business_keywords', {'query': '', 'limit': 1, 'path': '/tmp'})


if __name__ == '__main__':
    unittest.main()
