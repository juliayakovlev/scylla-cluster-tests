"""Tests for architecture validation in offline_install_scylla."""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseNode, NodeSetupFailed


class TestOfflineInstallArchValidation:
    """Validate that offline_install_scylla detects architecture mismatches early."""

    @staticmethod
    def _make_node(uname_output: str):
        """Create a mock node with a remoter that returns a given uname -m output."""
        node = MagicMock()
        uname_result = MagicMock()
        uname_result.stdout = uname_output
        node.remoter.run.return_value = uname_result
        return node

    def test_aarch64_package_on_x86_node_raises(self):
        """aarch64 unified package on an x86_64 node must raise NodeSetupFailed."""

        node = self._make_node("x86_64\n")
        unified_package = (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/"
            "scylla-unified-2026.1.1~dev-0.20260326.aarch64.tar.gz"
        )
        with pytest.raises(NodeSetupFailed, match="Architecture mismatch.*aarch64.*x86_64"):
            BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)

    def test_x86_package_on_aarch64_node_raises(self):
        """x86_64 unified package on an aarch64 node must raise NodeSetupFailed."""

        node = self._make_node("aarch64\n")
        unified_package = (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/"
            "scylla-unified-2026.1.1~dev-0.20260326.x86_64.tar.gz"
        )
        with pytest.raises(NodeSetupFailed, match="Architecture mismatch.*x86_64.*aarch64"):
            BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)

    def test_matching_x86_package_on_x86_node_passes(self):
        """x86_64 unified package on an x86_64 node should pass the arch check."""

        node = self._make_node("x86_64\n")
        unified_package = (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/"
            "scylla-unified-2026.1.1~dev-0.20260326.x86_64.tar.gz"
        )
        # The method should proceed past arch validation and call curl to download.
        # We don't need it to succeed fully, just not raise NodeSetupFailed.
        try:
            BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)
        except NodeSetupFailed:
            pytest.fail("NodeSetupFailed raised for matching architecture (x86_64)")
        except Exception:  # noqa: BLE001
            # Any other exception is fine – it means we passed the arch check
            pass

    def test_matching_aarch64_package_on_aarch64_node_passes(self):
        """aarch64 unified package on an aarch64 node should pass the arch check."""

        node = self._make_node("aarch64\n")
        unified_package = (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/"
            "scylla-unified-2026.1.1~dev-0.20260326.aarch64.tar.gz"
        )
        try:
            BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)
        except NodeSetupFailed:
            pytest.fail("NodeSetupFailed raised for matching architecture (aarch64)")
        except Exception:  # noqa: BLE001
            pass
