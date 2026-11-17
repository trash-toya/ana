# db/test — P0 test harness

## How to run

```bash
docker compose -f ops/docker-compose.yml up -d   # or: npm run db:up
npm run test:db                                  # all tests (T1–T9, invariants, property)
npm run test:db:concurrency                      # T1–T9 only
npm run test:property                            # property test (short seed set)
```

`DATABASE_URL` defaults to `postgresql://postgres:postgres@127.0.0.1:5432/postgres`.

## Conventions

- **One database per test file**, created from migrations and dropped afterwards
  (`harness.mjs#withTestDb`). Databases are named `sa_test_<label>_<id>` and are
  dropped even on failure (drop is in `finally`).
- **The harness applies every migration except `0014_schedule.sql`**, then asserts that
  none of `archive_sessions` / `rebuild_session_stats` / `cleanup_idempotency` /
  `reconcile_session_stats` is scheduled. It also **deletes every Timescale
  compression/retention policy job** (`policy_*`): tests write data months in the past,
  and the 90-day retention policy would otherwise drop the chunks under the test
  mid-run. Test databases are fully hermetic — no background job mutates state.
- **Role split during migrations**: `0001` and `0002` run as the bootstrap role
  (extension and role creation need it). Every later migration does
  `SET ROLE app_owner` itself and `RESET ROLE` at the end, so all tables and
  procedures are owned by `app_owner`, and `ingest_batch()` is re-owned by
  `app_writer` in `0009`. In production the migration runner connects with a role
  that is a member of `app_owner`.
  P4 gotchas: ownership transfers (`0011` moves the 0002 tables; `0015`/`0017`
  move functions to `app_writer`) must run **before** `SET ROLE app_owner` —
  transferring requires the current owner — so those migrations `RESET ROLE`
  around the `ALTER ... OWNER TO` statements. RLS changes are refused on
  compressed hypertables and CAGG materialization hypertables on the pinned
  image (see DEVIATIONS.md D37); do not "fix" the migrations to retry them.
- **`\i` paths are resolved relative to the repo root**: the runner spawns `psql`
  with `cwd` set to the repo root. Run tests through npm scripts so this holds.
- Tests never call the API for outcomes: they call `ingest_batch()` directly and
  assert on the returned JSON (`stored` / `dups` / `capped` / `conflicts`) plus the
  tables. Mapping outcomes to HTTP semantics is a P1 API test.

## Clock control

`SET`-based fake "now" is not possible for `now()`. Tests pre-create `session_guard`
rows with the desired `home_day`, pass explicit event `time` values, and
`UPDATE touch_bucket` / `created_at` / `received_at` where a job must see age.
`ingest_batch()` overwrites `home_day` only for new guards (`ON CONFLICT DO NOTHING`).

## Invariants

After every test the harness runs the invariant queries in `sql/`
(I1–I4, I6, I8; I5/I7/I9 have their own fixtures — see 06-testing.md).
