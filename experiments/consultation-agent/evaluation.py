"""Paired operational metrics; never turn structural validity into accuracy."""
import collections
import statistics

ARMS=['single','fixed','budget_review','adaptive']

def regression_checks(row,expected):
    pred=row.get('prediction') or {}
    names={k['name'] for k in pred.get('keywords',[])}
    missing=sorted(set(expected['required'])-names)
    forbidden=sorted(set(expected['forbidden'])&names)
    sentiment_match=pred.get('sentiment')==expected['sentiment']
    return {'missing':missing,'forbidden':forbidden,'sentiment_match':sentiment_match,'pass':not missing and not forbidden and sentiment_match,'kind':'authored_development_constraints_not_human_gold'}

def summarize(rows,receipts,case_ids,expected=None):
    if len(case_ids)!=len(set(case_ids)):raise ValueError('duplicate_case_ids')
    pairs={(r['id'],r['arm']):r for r in rows}
    if len(pairs)!=len(rows) or set(pairs)!={(i,a) for i in case_ids for a in ARMS}:raise ValueError('missing_duplicate_or_extra_pairs')
    for i in case_ids:
        group=[pairs[(i,a)] for a in ARMS]
        if len({r['base_payload_hash'] for r in group})!=1:raise ValueError('nonidentical_base_payload')
        if any(r['base_raw']!=group[0]['base_raw'] for r in group):raise ValueError('nonidentical_initial_response')
    output={'cases':len(case_ids),'arms':{},'human_gold_verified':0,'actual_success_receipt_cost_usd':sum(r['cost'] or 0 for r in receipts if r['status']=='COMPLETE'),
      'actual_success_calls':sum(r['status']=='COMPLETE' for r in receipts),'uncertain_calls':sum(r['status']!='COMPLETE' for r in receipts),
      'fairness':{'same_initial_payload_and_response':True,'shared_final_contract':True,'complete_input_and_dictionary_all_arms':True,'equal_max_model_calls_for_budget_review_and_adaptive':True,'same_actual_token_budget':False},
      'limitations':['Shared initial completion design; logical arm costs include the reused base receipt','Request seconds include historical shared receipt latency; current elapsed excludes MCP session startup','No human semantic gold; authored regression checks are development-only','Equal call ceilings are not equal realized token spend'],
      'usefulness_gate':{'passed':False,'status':'pending_external_evidence_review','reason':'Structural metrics and authored development constraints alone do not establish useful adaptive-agent superiority or semantic accuracy.'}}
    for arm in ARMS:
        group=[pairs[(i,arm)] for i in case_ids]
        output['arms'][arm]={'structural_valid':sum(r['prediction'] is not None for r in group),'validated_status':sum(r['status']=='VALIDATED' for r in group),
          'review_required':sum(r['status']=='REVIEW_REQUIRED' for r in group),'estimated_usd':sum(r['estimated_usd'] for r in group),'uncached_usd':sum(r['uncached_usd'] for r in group),
          'logical_model_calls':sum(r['model_calls'] for r in group),'tool_calls':sum(r['tool_calls'] for r in group),'tool_errors':sum(r['tool_errors'] for r in group),
          'mean_logical_request_seconds':statistics.mean(r['request_seconds'] for r in group),'mean_current_elapsed_seconds':statistics.mean(r['elapsed_seconds'] for r in group),
          'tool_seconds':sum(t.get('seconds',0) for r in group for t in r['trace'] if t['type']=='tool'),
          'stop_reasons':dict(collections.Counter(r.get('stop_reason','fixture') for r in group))}
        if expected:
            scores=[regression_checks(pairs[(i,arm)],expected[i]) for i in case_ids]
            output['arms'][arm]['authored_development_constraints_pass']=sum(x['pass'] for x in scores)
    return output
