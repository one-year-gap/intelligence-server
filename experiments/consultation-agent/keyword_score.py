"""Keyword-only scoring against supplied labels. Sentiment intentionally excluded."""
def score(rows,gold):
    if len(rows)!=len(gold) or len({r['id'] for r in rows})!=len(rows) or {r['id'] for r in rows}!=set(gold):
        raise ValueError('incomplete_or_duplicate_pairs')
    tp=fp=fn=exact=review=0
    details=[]
    for r in rows:
        expected=set(gold[r['id']]);pred=r.get('prediction')
        names={x['name'] for x in (pred or {}).get('keywords',[])}
        held=r['status']!='VALIDATED' or pred is None
        # All-case quality counts abstention as no delivered labels, never free success.
        delivered=set() if held else names
        a,b,c=len(delivered&expected),len(delivered-expected),len(expected-delivered)
        tp+=a;fp+=b;fn+=c;review+=held
        ok=not held and delivered==expected;exact+=ok
        details.append({'id':r['id'],'exact':ok,'review_required':held,'missing':sorted(expected-delivered),'extra':sorted(delivered-expected),'raw_predicted_keywords':sorted(names)})
    return {'cases':len(rows),'tp':tp,'fp':fp,'fn':fn,'micro_precision':tp/(tp+fp) if tp+fp else None,'micro_recall':tp/(tp+fn) if tp+fn else None,'micro_f1':2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else None,'exact_matches':exact,'review_required':review,'details':details}
