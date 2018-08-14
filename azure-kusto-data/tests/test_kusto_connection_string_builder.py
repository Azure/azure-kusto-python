"""Tests for KustoConnectionStringBuilder."""

from uuid import uuid4
import unittest
from azure.kusto.data.request import KustoConnectionStringBuilder


class KustoConnectionStringBuilderTests(unittest.TestCase):
    """Tests class for KustoConnectionStringBuilder."""

    def test_no_credentials(self):
        """Checks kcsb that is created with no credentials"""
        kcsbs = [
            KustoConnectionStringBuilder("localhost"),
            KustoConnectionStringBuilder("Data Source=localhost"),
            KustoConnectionStringBuilder("Addr=localhost"),
            KustoConnectionStringBuilder("Addr = localhost"),
            KustoConnectionStringBuilder.with_aad_device_authentication("localhost"),
        ]

        for kcsb in kcsbs:
            self._validate_kcsb_without_credentials(kcsb, "localhost")

    def _validate_kcsb_without_credentials(self, kcsb, data_source):
        self.assertEqual(kcsb.data_source, data_source)
        self.assertIsNone(kcsb.aad_user_id)
        self.assertIsNone(kcsb.password)
        self.assertIsNone(kcsb.application_client_id)
        self.assertIsNone(kcsb.application_key)
        self.assertIsNone(kcsb.authority_id)

    def test_aad_app(self):
        """Checks kcsb that is created with AAD application credentials."""
        uuid = str(uuid4())
        key = "key of application"
        kcsbs = [
            KustoConnectionStringBuilder(
                "localhost;Application Client Id={0};Application Key={1}".format(uuid, key)
            ),
            KustoConnectionStringBuilder(
                "Data Source=localhost ; Application Client Id={0}; AppKey ={1}".format(uuid, key)
            ),
            KustoConnectionStringBuilder(
                " Addr = localhost ; AppClientId = {0} ; AppKey ={1}".format(uuid, key)
            ),
            KustoConnectionStringBuilder(
                "Network Address = localhost; AppClientId = {0} ; AppKey ={1}".format(uuid, key)
            ),
            KustoConnectionStringBuilder.with_aad_application_key_authentication(
                "localhost", uuid, key
            ),
        ]

        kcsb1 = KustoConnectionStringBuilder("Server=localhost")
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.application_client_id] = uuid
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.application_key] = key
        kcsbs.append(kcsb1)

        kcsb2 = KustoConnectionStringBuilder("Server=localhost")
        kcsb2["AppClientId"] = uuid
        kcsb2["Application Key"] = key
        kcsbs.append(kcsb2)

        for kcsb in kcsbs:
            self._validate_kcsb_with_aad_app(kcsb, "localhost", uuid, key)

    def _validate_kcsb_with_aad_app(self, kcsb, data_source, app_id, key):
        self.assertEqual(kcsb.data_source, data_source)
        self.assertIsNone(kcsb.aad_user_id)
        self.assertIsNone(kcsb.password)
        self.assertEqual(kcsb.application_client_id, app_id)
        self.assertEqual(kcsb.application_key, key)
        self.assertIsNone(kcsb.authority_id)

    def test_aad_user(self):
        """Checks kcsb that is created with AAD user credentials."""
        user = "test"
        password = "Pa$$w0rd"
        kcsbs = [
            KustoConnectionStringBuilder(
                "localhost;AAD User ID={0};Password={1}".format(user, password)
            ),
            KustoConnectionStringBuilder(
                "Data Source=localhost ; AAD User ID={0}; Password ={1}".format(user, password)
            ),
            KustoConnectionStringBuilder(
                " Addr = localhost ; AAD User ID = {0} ; Pwd ={1}".format(user, password)
            ),
            KustoConnectionStringBuilder(
                "Network Address = localhost; AAD User ID = {0} ; Pwd = {1} ".format(user, password)
            ),
            KustoConnectionStringBuilder.with_aad_user_password_authentication(
                "localhost", user, password
            ),
        ]

        kcsb1 = KustoConnectionStringBuilder("Server=localhost")
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.aad_user_id] = user
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.password] = password
        kcsbs.append(kcsb1)

        kcsb2 = KustoConnectionStringBuilder("Server=localhost")
        kcsb2["AAD User ID"] = user
        kcsb2["Password"] = password
        kcsbs.append(kcsb2)

        for kcsb in kcsbs:
            self._validate_kcsb_with_aad_user(kcsb, "localhost", user, password)

    def _validate_kcsb_with_aad_user(self, kcsb, data_source, user_id, password):
        self.assertEqual(kcsb.data_source, data_source)
        self.assertEqual(kcsb.aad_user_id, user_id)
        self.assertEqual(kcsb.password, password)
        self.assertIsNone(kcsb.application_client_id)
        self.assertIsNone(kcsb.application_key)
        self.assertIsNone(kcsb.authority_id)
