"""Explicit offline RunStore factory DI; no authority or kernel proof grants."""

from contextlib import contextmanager
from unittest.mock import patch

from p7_fence_fixture import fixture_command_runner


@contextmanager
def explicit_runstore_factory(module):
    """Bind only this caller-selected module's original constructor in scope."""
    original = module.RunStore

    def constructor(*args, **kwargs):
        kwargs.setdefault("command_runner", fixture_command_runner)
        return original(*args, **kwargs)

    with patch.object(module, "RunStore", new=constructor):
        yield
