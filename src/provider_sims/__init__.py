"""Fictional transport providers, each a small standalone service.

They share nothing with ``orchestrator`` (enforced by import-linter). Each keeps its own SQLite
state so recovery tests cannot pass by the provider forgetting things, and each exposes a chaos
endpoint and named failpoints so failure sequences are deterministic, not probabilistic.
"""
