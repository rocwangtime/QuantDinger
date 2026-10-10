# Research and execution reliability scope

This scoped design implements the P0/P1 work agreed in the local development
conversation on 2026-10-06. It extends existing runtimes rather than replacing
the broker, strategy, or pending-order engines.

## Contracts

- Evolution submissions freeze the authorized source before entering the job
  queue. Workers freeze complete input panels, universe membership and
  instrument rules into a content-addressed bundle. Replays load that bundle
  and never fetch replacement market/fundamental data.
- Research history records cumulative attempted trials and holdout exposure.
  Missing or reused evidence cannot qualify a strategy for promotion. Fixed
  parameter window validation is explicitly named; it is not rolling refitting.
  PBO uses aligned daily return CSCV and retains fold ranking as a separate
  diagnostic. Deployment can require matching research evidence.
- AI entry policy supports `shadow`, `advisory` (legacy behavior), and
  `required`. Shadow preserves the raw suggestion without blocking. Required
  rejects entries when no valid decision is available. Exits bypass AI.
- AI reports use observed prices and hypothetical net-cost exits with an explicit horizon. They
  disclose selection bias and missing counterfactuals; model confidence is not
  treated as a calibrated market probability.
- Order groups initially execute only in isolated signal-mode virtual accounts.
  Each leg is durable and idempotent. Failed/expired groups cancel unsent work,
  reconcile partial fills, and require an explicit unwind or manual decision.
  No new real-account authority is introduced.
- Portfolio risk reports align dated return observations, shrink covariance,
  disclose insufficient data, and include gross/net exposure and stress loss.
  An opt-in entry guard serializes account admission, reserves queued risk, and
  rechecks the model before fresh broker submission. Exits and known broker
  identity recovery bypass admission.
- Recovery checks exercise partial fills, duplicate events, restart, uncertain
  submission, cancellation, and reconciliation. Local tests do not certify a
  deployed broker connection or profitable strategy.

## Acceptance

Focused regression tests cover frozen replay after changing providers/source,
cumulative selection accounting, missing/reused holdouts, AI provider and
billing failures in each mode, user isolation, multi-leg failures/recovery,
and correlated portfolio risk. Human OpenAPI is regenerated. Existing broker
authorization, reduce-only paths, and single-order behavior are preserved.
