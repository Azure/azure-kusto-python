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
            KustoConnectionStringBuilder("data Source=localhost"),
            KustoConnectionStringBuilder("Addr=localhost"),
            KustoConnectionStringBuilder("Addr = localhost"),
            KustoConnectionStringBuilder.with_aad_device_authentication("localhost"),
        ]

        for kcsb in kcsbs:
            self.assertEqual(kcsb.data_source, "localhost")
            self.assertIsNone(kcsb.aad_user_id)
            self.assertIsNone(kcsb.password)
            self.assertIsNone(kcsb.application_client_id)
            self.assertIsNone(kcsb.application_key)
            self.assertEqual(kcsb.authority_id, "common")

    def test_aad_app(self):
        """Checks kcsb that is created with AAD application credentials."""
        uuid = str(uuid4())
        key = "key of application"
        kcsbs = [
            KustoConnectionStringBuilder("localhost;Application client Id={0};application Key={1}".format(uuid, key)),
            KustoConnectionStringBuilder(
                "Data Source=localhost ; Application Client Id={0}; Appkey ={1}".format(uuid, key)
            ),
            KustoConnectionStringBuilder(" Addr = localhost ; AppClientId = {0} ; AppKey ={1}".format(uuid, key)),
            KustoConnectionStringBuilder(
                "Network Address = localhost; AppClientId = {0} ; AppKey ={1}".format(uuid, key)
            ),
            KustoConnectionStringBuilder.with_aad_application_key_authentication("localhost", uuid, key),
        ]

        kcsb1 = KustoConnectionStringBuilder("server=localhost")
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.application_client_id] = uuid
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.application_key] = key
        kcsbs.append(kcsb1)

        kcsb2 = KustoConnectionStringBuilder("Server=localhost")
        kcsb2["AppclientId"] = uuid
        kcsb2["Application key"] = key
        kcsbs.append(kcsb2)

        for kcsb in kcsbs:
            self.assertEqual(kcsb.data_source, "localhost")
            self.assertIsNone(kcsb.aad_user_id)
            self.assertIsNone(kcsb.password)
            self.assertEqual(kcsb.application_client_id, uuid)
            self.assertEqual(kcsb.application_key, key)
            self.assertEqual(kcsb.authority_id, "common")

    def test_aad_user(self):
        """Checks kcsb that is created with AAD user credentials."""
        user = "test"
        password = "Pa$$w0rd"
        kcsbs = [
            KustoConnectionStringBuilder("localhost;AAD User ID={0};password={1}".format(user, password)),
            KustoConnectionStringBuilder(
                "Data Source=localhost ; AaD User ID={0}; Password ={1}".format(user, password)
            ),
            KustoConnectionStringBuilder(" Addr = localhost ; AAD User ID = {0} ; Pwd ={1}".format(user, password)),
            KustoConnectionStringBuilder(
                "Network Address = localhost; AAD User iD = {0} ; Pwd = {1} ".format(user, password)
            ),
            KustoConnectionStringBuilder.with_aad_user_password_authentication("localhost", user, password),
        ]

        kcsb1 = KustoConnectionStringBuilder("Server=localhost")
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.aad_user_id] = user
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.password] = password
        kcsbs.append(kcsb1)

        kcsb2 = KustoConnectionStringBuilder("server=localhost")
        kcsb2["AAD User ID"] = user
        kcsb2["Password"] = password
        kcsbs.append(kcsb2)

        for kcsb in kcsbs:
            self.assertEqual(kcsb.data_source, "localhost")
            self.assertEqual(kcsb.aad_user_id, user)
            self.assertEqual(kcsb.password, password)
            self.assertIsNone(kcsb.application_client_id)
            self.assertIsNone(kcsb.application_key)
            self.assertEqual(kcsb.authority_id, "common")

    def test_aad_user_with_authority(self):
        """Checks kcsb that is created with AAD user credentials."""
        user = "test2"
        password = "Pa$$w0rd2"
        authority_id = "13456"

        kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(
                "localhost", user, password, authority_id
            )
    
        self.assertEqual(kcsb.data_source, "localhost")
        self.assertEqual(kcsb.aad_user_id, user)
        self.assertEqual(kcsb.password, password)
        self.assertIsNone(kcsb.application_client_id)
        self.assertIsNone(kcsb.application_key)
        self.assertEqual(kcsb.authority_id, authority_id)
