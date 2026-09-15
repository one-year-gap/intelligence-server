"""Read-only, ID-scoped local tools shared by direct and MCP transports."""
import copy
import hashlib
import json
from pathlib import Path


def _schema(properties):
    return {'type': 'object', 'properties': properties, 'required': list(properties), 'additionalProperties': False}


TOOL_SCHEMAS = [
    {'name': 'get_consultation_turns', 'description': 'Read selected consultation turns and bounded neighboring context from the frozen manifest.',
     'inputSchema': _schema({'consultation_id': {'type': 'string', 'minLength': 1, 'maxLength': 200}, 'turn_ids': {'type': 'array', 'items': {'type': 'integer'}, 'minItems': 1, 'maxItems': 20}, 'window': {'type': 'integer', 'minimum': 0, 'maximum': 3}})},
    {'name': 'lookup_business_keywords', 'description': 'Look up canonical keyword names and aliases in the frozen dictionary.',
     'inputSchema': _schema({'query': {'type': 'string', 'maxLength': 200}, 'limit': {'type': 'integer', 'minimum': 1, 'maximum': 20}})},
    {'name': 'validate_analysis', 'description': 'Check structure, dictionary membership and customer-turn references; reconstruct exact evidence. Does not verify semantic correctness.',
     'inputSchema': _schema({'consultation_id': {'type': 'string', 'minLength': 1, 'maxLength': 200}, 'analysis_json': {'type': 'string', 'maxLength': 20000}})},
]


def tool_identity(manifest_bytes, taxonomy_bytes):
    """Same immutable data/code namespace for direct and MCP transports."""
    source_dir = Path(__file__).resolve().parent
    material = {'manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
                'taxonomy_sha256': hashlib.sha256(taxonomy_bytes).hexdigest(),
                'schema_sha256': hashlib.sha256(json.dumps(TOOL_SCHEMAS, sort_keys=True).encode()).hexdigest(),
                'sources': {name: hashlib.sha256((source_dir / name).read_bytes()).hexdigest()
                            for name in ('tool_backend.py', 'mcp_server.py', 'mcp_transport.py', 'contract.py')}}
    return 'holliverse-tools-v1:' + hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()


class ToolBackend:
    def __init__(self, manifest_path, taxonomy_path):
        manifest_bytes = Path(manifest_path).read_bytes()
        body = manifest_bytes.decode('utf-8')
        if Path(manifest_path).suffix == '.jsonl':
            rows = [json.loads(line) for line in body.splitlines() if line.strip()]
        else:
            rows = json.loads(body)
        if not isinstance(rows, list):
            raise ValueError('manifest must contain a list of records')
        self.records = {}
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get('id'), str) or not isinstance(row.get('text'), str):
                raise ValueError('manifest records require string id and text')
            if row['id'] in self.records or not row['id']:
                raise ValueError('duplicate or empty consultation ID')
            actual = hashlib.sha256(row['text'].encode('utf-8')).hexdigest()
            if row.get('input_sha256') != actual:
                raise ValueError('manifest input hash mismatch')
            self.records[row['id']] = {'text': row['text'], 'input_sha256': actual}
        taxonomy_bytes = Path(taxonomy_path).read_bytes()
        self.taxonomy = json.loads(taxonomy_bytes)
        if not isinstance(self.taxonomy, dict) or any(not isinstance(k, str) or not isinstance(v, list) or any(not isinstance(a, str) for a in v) for k, v in self.taxonomy.items()):
            raise ValueError('taxonomy must map names to string alias lists')
        self.taxonomy_version = hashlib.sha256(taxonomy_bytes).hexdigest()
        self.identity = tool_identity(manifest_bytes, taxonomy_bytes)

    def list_tools(self):
        return copy.deepcopy(TOOL_SCHEMAS)

    def assert_case(self, case, taxonomy):
        if not isinstance(case, dict) or not isinstance(case.get('id'), str) or not isinstance(case.get('text'), str):
            raise ValueError('invalid case identity')
        row = self._record(case['id'])
        if hashlib.sha256(case['text'].encode('utf-8')).hexdigest() != row['input_sha256']:
            raise ValueError('case text differs from tool snapshot')
        if taxonomy != self.taxonomy:
            raise ValueError('taxonomy differs from tool snapshot')

    def dispatch(self, name, args):
        spec = next((s for s in TOOL_SCHEMAS if s['name'] == name), None)
        if spec is None:
            raise ValueError('unknown tool')
        if not isinstance(args, dict) or set(args) != set(spec['inputSchema']['required']):
            raise ValueError('unexpected or missing tool arguments')
        for key, prop in spec['inputSchema']['properties'].items():
            value = args[key]
            if prop['type'] == 'string':
                if not isinstance(value, str) or not prop.get('minLength', 0) <= len(value) <= prop['maxLength']:
                    raise ValueError(f'invalid {key}')
            elif prop['type'] == 'integer':
                if type(value) is not int or not prop['minimum'] <= value <= prop['maximum']:
                    raise ValueError(f'invalid {key}')
            elif not isinstance(value, list) or not prop['minItems'] <= len(value) <= prop['maxItems'] or any(type(i) is not int for i in value):
                raise ValueError(f'invalid {key}')
        return getattr(self, name)(**args)

    def _record(self, consultation_id):
        if consultation_id not in self.records:
            raise ValueError('unknown consultation ID')
        return self.records[consultation_id]

    def get_consultation_turns(self, consultation_id, turn_ids, window):
        from contract import turns
        row = self._record(consultation_id)
        parsed = turns(row['text'])
        positions = {turn['id']: i for i, turn in enumerate(parsed)}
        if any(tid not in positions for tid in turn_ids):
            raise ValueError('unknown turn ID')
        selected = set()
        for tid in turn_ids:
            position = positions[tid]
            selected.update(range(max(0, position - window), min(len(parsed), position + window + 1)))
        return {'consultation_id': consultation_id, 'input_sha256': row['input_sha256'], 'turns': [parsed[i] for i in sorted(selected)]}

    def lookup_business_keywords(self, query, limit):
        query = query.casefold().strip()
        found = [{'name': name, 'aliases': list(aliases)} for name, aliases in self.taxonomy.items() if any(query in term.casefold() for term in [name, *aliases])]
        return {'keywords': found[:limit], 'taxonomy_version': self.taxonomy_version}

    def validate_analysis(self, consultation_id, analysis_json):
        from contract import resolve
        row = self._record(consultation_id)
        try:
            raw = json.loads(analysis_json)
            resolved = resolve(raw, row['text'], self.taxonomy)
        except (ValueError, TypeError) as exc:
            return {'valid': False, 'errors': [str(exc)], 'semantic_verified': False}
        return {'valid': True, 'errors': [], 'semantic_verified': False, 'resolved': resolved}
