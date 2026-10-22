"""Optimized private ladder keeps the public ladder's timestamp semantics."""
from datetime import datetime, timezone
from app.services.polymarket import observer
from app.services.polymarket.ws_payloads import loads, feed_envelope
from app.services.polymarket.ws_warm_book import WarmBook
from app.services.polymarket.ws_redundancy import FeedMerge
import json


def test_private_book_matches_merged_oracle_with_deletes_and_out_of_order():
    warm=WarmBook({'a'});merge=FeedMerge({});books={}
    events=[dict(event_type='book',asset_id='a',timestamp=1000,hash='base',bids=[dict(price='.4',size='2')],asks=[])]
    for t,side,price,size in [(1100,'BUY','.4','9'),(1050,'BUY','.3','4'),(1080,'BUY','.4','1'),(1200,'BUY','.3','0')]:
        events.append(dict(event_type='price_change',timestamp=t,price_changes=[dict(asset_id='a',side=side,price=price,size=size,hash=str(t))]))
    for event in events:
        warm.receive(loads(json.dumps(event)),datetime.now(timezone.utc))
        filtered=merge.filter(event,'main',{'a'})
        observer.ws_apply_event(filtered,books)
    assert warm.books==books=={'a':{'bids':{'.4':'9'},'asks':{}}}
    assert warm.checkpoints.latest['a'][1]=='1200'


def test_ingress_envelope_handles_wrapped_camel_case_public_payload():
    event=dict(type='price_change',payload=dict(timestamp=1000,priceChanges=[dict(assetId='a',hash='h')]))
    assert feed_envelope(event)==[('price_change','a',1000,'h')]
