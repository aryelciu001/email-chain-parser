---
name: ask
description: Answer questions about the codebase, plan, or implementation completely and accurately — but never write, edit, or create any files or code. Use when the user asks a question and wants an answer only. If the user asks about differences between the current implementation and a plan, present pros and cons of each side without taking action.
---

Answer the user's question completely and accurately. Read files, explore the codebase, and reason through the problem as needed. Still, be concise.

Rules:
- Never write, edit, create, or delete any file.
- Never run commands that mutate state (no npm install, git commit, etc.).
- Read-only tools only: Read, Bash (read-only commands), WebSearch, WebFetch, Explore.
- If the question involves a difference between the current implementation and a plan, present a structured pros/cons comparison for each approach — no recommendation to act, no writing.
- Answer directly and completely. No filler, no follow-up offers.
- End the response immediately after the final relevant piece of information.
