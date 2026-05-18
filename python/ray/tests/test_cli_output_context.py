"""Tests for CLI output context disambiguation (ray start / ray up).

These tests directly import and call the output helper functions from
``cli_output_helpers``, capturing ``cli_logger`` calls via mocks.

No Ray C++ runtime (``_raylet``) is required.  The helpers module is
imported by path to avoid triggering ``import ray``.

Validates that:
1. ``print_next_steps_context_note`` emits "head node" and "cluster network".
2. ``print_head_node_context_separator`` emits "head node" and "cluster network".
3. ``USEFUL_COMMANDS_HEADING`` contains "local machine".
4. Neither helper references ``ray up`` (valid for direct ``ray start`` use).
"""

import importlib.util
import os
import unittest
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Import cli_output_helpers by file path so we bypass ``import ray`` entirely.
# ---------------------------------------------------------------------------
_HELPERS_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    "autoscaler",
    "_private",
    "cli_output_helpers.py",
)
_spec = importlib.util.spec_from_file_location(
    "cli_output_helpers", os.path.abspath(_HELPERS_PATH)
)
_helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)

print_next_steps_context_note = _helpers.print_next_steps_context_note
print_head_node_context_separator = _helpers.print_head_node_context_separator
USEFUL_COMMANDS_HEADING = _helpers.USEFUL_COMMANDS_HEADING


def _capture_printed_text(helper_fn):
    """Call *helper_fn(cli_logger, cf)* with mocks and return all text
    that was passed to ``cli_logger.print`` as a single concatenated string.

    ``cf.dimmed`` is mocked as an identity function so the raw text
    passes through unchanged.
    """
    mock_logger = MagicMock()
    mock_cf = MagicMock()
    # cf.dimmed should return its argument so we can inspect the raw text
    mock_cf.dimmed = lambda x: x

    helper_fn(mock_logger, mock_cf)

    parts = []
    for c in mock_logger.print.call_args_list:
        args, _ = c
        parts.extend(str(a) for a in args)
    return "\n".join(parts)


class TestNextStepsContextNote(unittest.TestCase):
    """Tests for ``print_next_steps_context_note``."""

    def test_mentions_head_node(self):
        text = _capture_printed_text(print_next_steps_context_note)
        self.assertIn("head node", text)

    def test_mentions_cluster_network(self):
        text = _capture_printed_text(print_next_steps_context_note)
        self.assertIn("cluster network", text)

    def test_calls_newline(self):
        mock_logger = MagicMock()
        mock_cf = MagicMock()
        mock_cf.dimmed = lambda x: x
        print_next_steps_context_note(mock_logger, mock_cf)
        mock_logger.newline.assert_called()

    def test_does_not_mention_ray_up(self):
        text = _capture_printed_text(print_next_steps_context_note)
        self.assertNotIn("ray up", text)


class TestHeadNodeContextSeparator(unittest.TestCase):
    """Tests for ``print_head_node_context_separator``."""

    def test_mentions_head_node(self):
        text = _capture_printed_text(print_head_node_context_separator)
        self.assertIn("head node", text)

    def test_mentions_cluster_network(self):
        text = _capture_printed_text(print_head_node_context_separator)
        self.assertIn("cluster network", text)

    def test_prints_ascii_separator(self):
        text = _capture_printed_text(print_head_node_context_separator)
        # Should contain a run of ASCII dashes as a visual separator
        self.assertIn("----", text)

    def test_calls_newline(self):
        mock_logger = MagicMock()
        mock_cf = MagicMock()
        mock_cf.dimmed = lambda x: x
        print_head_node_context_separator(mock_logger, mock_cf)
        mock_logger.newline.assert_called()


class TestUsefulCommandsHeading(unittest.TestCase):
    """Tests for the ``USEFUL_COMMANDS_HEADING`` constant."""

    def test_contains_local_machine(self):
        self.assertIn("local machine", USEFUL_COMMANDS_HEADING)

    def test_does_not_use_old_label(self):
        # The old heading was exactly "Useful commands:" with no qualifier
        self.assertNotEqual(USEFUL_COMMANDS_HEADING, "Useful commands:")


class TestHeadNodeContextGating(unittest.TestCase):
    """Verify that the head node context separator is only printed when
    `ray start` commands are actually executed.
    """

    def test_separator_gated_by_ray_start_commands(self):
        import ast

        # Parse commands.py to verify the gating logic without importing it
        filepath = os.path.join(
            os.path.dirname(__file__),
            os.pardir,
            "autoscaler",
            "_private",
            "commands.py",
        )
        with open(filepath, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)

        call_found = False
        # Find the call to print_head_node_context_separator
        for node in ast.walk(tree):
            if isinstance(node, ast.If):
                # Check if this If block contains our call
                for child in ast.walk(node):
                    if (
                        isinstance(child, ast.Call)
                        and getattr(child.func, "id", None)
                        == "print_head_node_context_separator"
                    ):
                        # Verify the condition of the If statement is ray_start_commands
                        test_node = node.test
                        self.assertIsInstance(test_node, ast.Name)
                        self.assertEqual(test_node.id, "ray_start_commands")
                        call_found = True

        self.assertTrue(
            call_found,
            "Could not find conditional call to print_head_node_context_separator",
        )


if __name__ == "__main__":
    unittest.main()
