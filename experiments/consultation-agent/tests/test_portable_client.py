import os
import unittest
from unittest.mock import patch
import experiment


class PortableClientTests(unittest.TestCase):
    def test_missing_key_fails_before_http_client_creation(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(experiment, "client") as client:
            with self.assertRaisesRegex(ValueError, "OPENAI_API_KEY"):
                experiment.client_from_env()
            client.assert_not_called()

    def test_environment_key_is_forwarded_without_file_access(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "fixture-key"}), patch.object(experiment, "client") as client:
            self.assertIs(experiment.client_from_env(), client.return_value)
            client.assert_called_once_with("fixture-key")
