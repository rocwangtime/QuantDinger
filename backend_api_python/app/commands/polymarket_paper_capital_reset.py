"""Reset a flat PAPER account with an immutable capital_reset ledger entry.

Production procedure (operator only): first stop polymarket-observer (or confirm
its paper worker is stopped), run python -m app.commands.polymarket_paper_capital_reset,
then start polymarket-observer. The shared worker lock rejects concurrent writes.
Never run this against a funded account; no wallet/order integration exists here.
"""
import argparse
import json
import time

from app.services.polymarket.paper_account import Store, configuration


def validate_account_id(value):
    if 'contaminated' in value.lower():
        raise ValueError('paper_capital_reset_contaminated_account_forbidden')
    if not value or len(value) > 80:
        raise ValueError('invalid_account_id')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account-id', default='global')
    parser.add_argument('--reason', default='roc_approved_2026-10-23')
    args = parser.parse_args()
    try:
        account_id = validate_account_id(args.account_id)
        detail = Store(account_id).reset_capital(configuration(), int(time.time() * 1000), reason=args.reason)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(dict(account_id=account_id, before=detail['before'], after=detail['after'],
                         reason=detail['reason'], actor=detail['actor']), ensure_ascii=False))


if __name__ == '__main__':
    main()
