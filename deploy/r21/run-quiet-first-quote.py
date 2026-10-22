"""Deterministic conditional false-promotion sweep, not an empirical live rate."""
import argparse,json,logging,random,time
from pathlib import Path
from unittest.mock import patch
from tests.test_polymarket_liveness_silence import _wired,_feed
from app.services.polymarket import observer


def run(cases=1000):
    rng=random.Random(21);clock=[100.0];promoted=0
    with patch.object(observer.time,'monotonic',lambda:clock[0]):
        for n in range(cases):
            clock[0]=100+n*30
            manager,primary,spare,*_=_wired()
            pong_age=rng.uniform(0,10)
            delay=rng.uniform(0,.8)
            primary['inbox'].last_byte_at=clock[0]-pong_age
            started=clock[0]
            _feed(manager,spare)
            for offset in (.1,.2,.4,.6):
                if offset<delay:
                    clock[0]=started+offset
                    _feed(manager,spare)
            promoted+=manager.states[0] is not primary
            clock[0]=started+delay
            primary['inbox'].last_byte_at=clock[0]
            _feed(manager,spare)
    return dict(cases=cases,seed=21,last_primary_pong_age_uniform_s=[0,10],normal_copy_delay_uniform_s=[0,.8],
        premature_promotions=promoted,conditional_false_promotion_ratio=promoted/cases,
        scope='Synthetic first quote after quiet; not a production false-promotion probability or confidence bound.')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    result=run();Path(args.output).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
