# Edge cases

The situations the platform is designed around, and the mechanism that handles each. The
numbers are referenced from code comments and tests. A mechanism named here is implemented
and covered by a test; where the honest outcome is an open review case, that is stated.

| # | Scenario | Mechanism |
|---|---|---|
| 1 | Same client request twice | Idempotency contract; unique constraint |
| 2 | Provider timed out but had created the booking | Unsettled possible effect to `UNKNOWN`; A and C settle by resubmission or fenced lookup; B by client-reference lookup, else review |
| 3 | Writer paused after its expiry check, before commit | Fenced lookup serializes with the writer: it is either seen or aborted |
| 4 | Provider admits and validates before expiry, commits after | Fenced lookup, same as #3 |
| 5 | Provider clock lags | Declared `max_clock_skew`; margins derived; `clock_offset_ms` |
| 6 | First attempt pauses past the dedupe horizon | The first attempt's expiry is below the cutoff, below the horizon |
| 7 | Expired-request rejection is lost | Settles only that attempt; command settled by predicate |
| 8 | Definitive rejection from B with no earlier possible effect | `REJECTED` immediately, basis `PROVIDER_RESULT` |
| 9 | Provider says the key is "in progress" | Uncertainty preserved |
| 10 | Provider returned success but our DB write failed | Unsettled possible effect; same as #2 |
| 11 | Crash after `CREATED` committed | `next_action_at`; worker submits |
| 12 | Crash between hold and confirm | `HELD` durable; deadline-aware confirm; expiry observed to `FAILED`, CREATE `REJECTED` |
| 13 | Confirm attempt expired, hold still live | Attempt excluded; new attempt inside the cutoff or replacement command within the booking's confirm budget |
| 14 | Hold with seconds left, admission rejected | Backoff capped; dedicated confirmation loop, pool slice, purpose |
| 15 | Every attempt rejected locally | Reschedule; `ABANDONED` after max age if never dispatched |
| 16 | Local rejection after an earlier `POSSIBLE` | Command certainty never regresses |
| 17 | Webhook arrives twice | Receipt unique in the applying transaction |
| 18 | Webhook and poll race | Generation then revision ordering under the row lock; one transition |
| 19 | Webhook for another booking's reference | `UNMATCHED` |
| 20 | Generation bump with an outstanding command | Outstanding work preserved; stale-generation response is attempt evidence only |
| 21 | Lower revision within the same generation on a read | Quarantined for review (`REGRESSED`) |
| 22 | Early webhook, late create response | Bound only after an authoritative read agrees with the event; the late response closes its attempt; CREATE settles on `CONFIRMED` |
| 23 | Discovered hold for an unbound CREATE | Binds; CREATE stays `OPEN`; 202 until confirmed |
| 24 | Discovered reservation already cancelled | Binds; booking `CANCELLED`; CREATE `SUCCEEDED` |
| 25 | Create key replayed while pending or held | 202 |
| 26 | Create key replayed after a later cancellation went to review | 201 with current state `NEEDS_REVIEW` |
| 27 | Cancel key reused for a different booking | 422 |
| 28 | Crash right after `CANCELLING`, before quote | Phase `NONE` recovery |
| 29 | Crash after quote, before acceptance | Phase `QUOTED` recovery |
| 30 | Acceptance admission rejected, then crash | Not `POSSIBLE`: resume |
| 31 | Acceptance timed out and recorded `UNKNOWN`, then crash | Per-attempt settlement by refund offer status (A) or retry plus lookup (C) |
| 32 | C cancel attempt expired, reservation still confirmed, cutoff not reached | New attempt with fresh short expiry inside the same command |
| 33 | C cancel cutoff reached, still confirmed | `REFUSED(EXPIRED)`; a later retry is a new command |
| 34 | Terms unacceptable before any acceptance | `TERMS_CHANGED` immediately |
| 35 | Quote expires while an acceptance may have committed | No re-quote until every acceptance attempt is settled |
| 36 | Review sees `CONFIRMED` while acceptance outstanding | Not settled until the exact refund offer's status is known |
| 37 | Two reservations discovered for an unbound CREATE | Processed as one set; review case lists both; no bind |
| 38 | Duplicate survives, bound reference cancelled | Case stays open; the duplicate is listed in the case for an operator to dispose of (`CANCEL_EXTRA` is modelled, not executed by the platform); no rebind |
| 39 | Duplicate at a provider without cancellation | Case marked non-remediable, stays open; the duplicate is visible to operators |
| 40 | Hold evidence expires during review | Validity bound; closure into `HELD` rejected; fresh evidence required; never `FAILED` by time alone |
| 41 | Operator tries to force a state the lookup contradicts | No override: 409 |
| 42 | Lookup fails during review | Case stays open |
| 43 | Review lookup superseded by a later dispatch | Rejected as evidence |
| 44 | B request never arrives | Journaled `POSSIBLE`; client-reference lookup forever negative; case may stay open; never `FAILED` |
| 45 | B commits, index never exposes it | Same; platform legitimately cannot see it |
| 46 | Reservation appears for a `FAILED` booking | `FAILED -> NEEDS_REVIEW` on verified evidence |
| 47 | Stale worker overwrites a newer worker | Lease fencing |
| 48 | Redis unavailable | All purposes fail closed; no presumed expiry; `quota-outage` reason |
| 49 | Slow lookups or cancellations flood the worker | Dedicated confirmation loop and pool slice; benchmark against the envelope |
| 50 | Search storm during hold confirmations | Separate purposes |
| 51 | Offer expired between search and booking | Local check; provider rejection definitive |
| 52 | Passengers differ from the offer | 409 before any provider call |
| 53 | Cancel while pending | 409 |
| 54 | Two API replicas process the same webhook | Same as #17: the receipt's unique constraint inside the applying transaction |
