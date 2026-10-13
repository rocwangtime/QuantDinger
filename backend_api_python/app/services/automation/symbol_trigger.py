"""Fresh market-session percentage edges and durable directional cooldowns."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.services.automation.domain import session_schedule


def session_allowed(config, now):
    p = config['symbol_params']
    local = datetime.fromtimestamp(now, ZoneInfo('America/New_York' if config['market'] == 'USStock' else 'Asia/Hong_Kong'))
    day = local.date().isoformat()
    # Calendar handles holidays and half-days; lunch/auction never authorize execution.
    schedule = session_schedule(config, local.replace(hour=0, minute=0, second=0).astimezone(timezone.utc))
    minute = local.hour * 60 + local.minute
    opening, closing = schedule['open_at'].timestamp(), schedule['close_at'].timestamp()
    regular = opening <= now < closing
    extended = (config['market'] == 'USStock' and p.get('extended_hours', False)
                and day == schedule['session'] and 240 <= minute < 1200)
    if (not regular and not extended) or (regular and (
            now < opening + p.get('blackout_open_minutes', 0)*60
            or now >= closing - p['blackout_close_minutes']*60)) or (
            config['market'] == 'HKStock' and 720 <= minute < 780):
        return False
    return True


def evaluate(config, quote, previous, now):
    p = config['symbol_params']
    state = dict(previous)
    local = datetime.fromtimestamp(now, ZoneInfo('America/New_York' if config['market'] == 'USStock' else 'Asia/Hong_Kong'))
    day = local.date().isoformat()
    if state.get('day') != day:
        state = {'day': day, 'count': 0, 'samples': [], 'cooldowns': {}}
    if not session_allowed(config, now):
        return False, state
    samples = [s for s in state.get('samples', []) if 0 <= now-s[0] <= p['window_minutes']*60+10]
    price = float(quote['price'])
    base = next((s[1] for s in samples if now-s[0] >= p['window_minutes']*60), None)
    prev = quote.get('prev_close')
    moves = []
    if prev and float(prev) > 0:
        moves.append((price/float(prev)-1)*100/p['prev_close_pct'])
    if base:
        moves.append((price/base-1)*100/p['short_move_pct'])
    direction = 'up' if any(m >= 1 for m in moves) else 'down' if any(m <= -1 for m in moves) else None
    cooldowns = dict(state.get('cooldowns', {}))
    fire = bool(direction and direction != state.get('latched') and now >= cooldowns.get(direction, 0) and state['count'] < p['max_triggers_day'])
    if fire:
        cooldowns[direction] = now+p['cooldown_minutes']*60
        state['count'] += 1
        state['reference'] = {'direction': direction, 'prev_close': prev, 'short_base': base}
    # Keep at most one observation per second and a bounded 120-minute history.
    if not samples or now-samples[-1][0] >= 1:
        samples.append([now, price])
    state.update(samples=samples[-7210:], cooldowns=cooldowns, latched=direction)
    return fire, state


def condition(p, price, reference):
    sign = 1 if reference.get('direction') == 'up' else -1
    return any(base and sign*(price/float(base)-1)*100 >= threshold
               for base, threshold in [(reference.get('prev_close'), p['prev_close_pct']),
                                       (reference.get('short_base'), p['short_move_pct'])])
