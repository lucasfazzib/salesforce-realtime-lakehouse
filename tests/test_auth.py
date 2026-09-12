import importlib
import unittest
from unittest.mock import patch

import src.salesforce_cdc.auth as auth


class AuthImportTests(unittest.TestCase):
    def test_import_does_not_load_dotenv(self) -> None:
        with patch("dotenv.load_dotenv") as load_dotenv:
            importlib.reload(auth)

        load_dotenv.assert_not_called()
        importlib.reload(auth)


if __name__ == "__main__":
    unittest.main()