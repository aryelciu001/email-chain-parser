applies to showing emails under a thread
1. create a mapping of `leader email -> dup email`
  leader email is an email that is not duplicate / similar (no `m` suffix)
  dup email: duplicate email or similar email (with suffix m)
2. invert that mapping to `dup email -> leader email`
3. for each canon order, maintain a mapping of `canon order -> list of leader email`
  this step should eliminate most of the emails, assuming most are duplicates
4. for email with parent_id, resolve the parent_id to a leader:
  - look up parent_id in `dup email -> leader email` mapping
  - if the result is still a dup, follow transitively until reaching a leader or null
  - use the resolved leader's id as the effective parent_id
5. for each canon order, if there are multiple leader emails, they are siblings —
  draw a line from their shared (resolved) parent to each sibling
6. this logic is implemented in the frontend using data already available from
  GET /api/threads?thread_id=... (fields: id, duplicate, parent_id, canon_order)
