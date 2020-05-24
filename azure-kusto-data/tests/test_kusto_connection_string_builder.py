# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import unittest
from uuid import uuid4

from azure.kusto.data import KustoConnectionStringBuilder


class KustoConnectionStringBuilderTests(unittest.TestCase):
    """Tests class for KustoConnectionStringBuilder."""

    PASSWORDS_REPLACEMENT = "****"

    def test_no_credentials(self):
        """Checks kcsb that is created with no credentials"""
        kcsbs = [
            KustoConnectionStringBuilder("localhost"),
            KustoConnectionStringBuilder("data Source=localhost"),
            KustoConnectionStringBuilder("Addr=localhost"),
            KustoConnectionStringBuilder("Addr = localhost"),
        ]

        for kcsb in kcsbs:
            assert kcsb.data_source == "localhost"
            assert not kcsb.aad_federated_security
            assert kcsb.aad_user_id is None
            assert kcsb.password is None
            assert kcsb.application_client_id is None
            assert kcsb.application_key is None
            assert kcsb.authority_id == "common"
            assert repr(kcsb) == "Data Source=localhost;Authority Id=common"
            assert str(kcsb) == "Data Source=localhost;Authority Id=common"

    def test_aad_app(self):
        """Checks kcsb that is created with AAD application credentials."""
        uuid = str(uuid4())
        key = "key of application"
        kcsbs = [
            KustoConnectionStringBuilder(
                "localhost;Application client Id={0};application Key={1};Authority Id={2} ; aad federated security = {3}".format(
                    uuid, key, "microsoft.com", True
                )
            ),
            KustoConnectionStringBuilder(
                "Data Source=localhost ; Application Client Id={0}; Appkey ={1};Authority Id= {2} ; aad federated security = {3}".format(
                    uuid, key, "microsoft.com", True
                )
            ),
            KustoConnectionStringBuilder(
                " Addr = localhost ; AppClientId = {0} ; AppKey ={1}; Authority Id={2} ; aad federated security = {3}".format(uuid, key, "microsoft.com", True)
            ),
            KustoConnectionStringBuilder(
                "Network Address = localhost; AppClientId = {0} ; AppKey ={1};AuthorityId={2} ; aad federated security = {3}".format(
                    uuid, key, "microsoft.com", True
                )
            ),
            KustoConnectionStringBuilder.with_aad_application_key_authentication("localhost", uuid, key, "microsoft.com"),
        ]

        try:
            KustoConnectionStringBuilder.with_aad_application_key_authentication("localhost", uuid, key, None)
        except Exception as e:
            # make sure error is raised when authority_id i none
            assert isinstance(e, ValueError)

        kcsb1 = KustoConnectionStringBuilder("server=localhost")
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.application_client_id] = uuid
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.application_key] = key
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.authority_id] = "microsoft.com"
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.aad_federated_security] = True
        kcsbs.append(kcsb1)

        kcsb2 = KustoConnectionStringBuilder("Server=localhost")
        kcsb2["AppclientId"] = uuid
        kcsb2["Application key"] = key
        kcsb2["Authority Id"] = "microsoft.com"
        kcsb2["aad federated security"] = True
        kcsbs.append(kcsb2)

        for kcsb in kcsbs:
            assert kcsb.data_source == "localhost"
            assert kcsb.aad_federated_security
            assert kcsb.aad_user_id is None
            assert kcsb.password is None
            assert kcsb.application_client_id == uuid
            assert kcsb.application_key == key
            assert kcsb.authority_id == "microsoft.com"
            assert repr(kcsb) == "Data Source=localhost;AAD Federated Security=True;Application Client Id={0};Application Key={1};Authority Id={2}".format(
                uuid, key, "microsoft.com"
            )

            assert str(kcsb) == "Data Source=localhost;AAD Federated Security=True;Application Client Id={0};Application Key={1};Authority Id={2}".format(
                uuid, self.PASSWORDS_REPLACEMENT, "microsoft.com"
            )

    def test_aad_user(self):
        """Checks kcsb that is created with AAD user credentials."""
        user = "test"
        password = "Pa$$w0rd"
        kcsbs = [
            KustoConnectionStringBuilder("localhost;AAD User ID={0};password={1} ;AAD Federated Security=True ".format(user, password)),
            KustoConnectionStringBuilder("Data Source=localhost ; AaD User ID={0}; Password ={1} ;AAD Federated Security=True".format(user, password)),
            KustoConnectionStringBuilder(" Addr = localhost ; AAD User ID = {0} ; Pwd ={1} ;AAD Federated Security=True".format(user, password)),
            KustoConnectionStringBuilder("Network Address = localhost; AAD User iD = {0} ; Pwd = {1} ;AAD Federated Security= True  ".format(user, password)),
            KustoConnectionStringBuilder.with_aad_user_password_authentication("localhost", user, password),
        ]

        kcsb1 = KustoConnectionStringBuilder("Server=localhost")
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.aad_user_id] = user
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.password] = password
        kcsb1[KustoConnectionStringBuilder.ValidKeywords.aad_federated_security] = True
        kcsbs.append(kcsb1)

        kcsb2 = KustoConnectionStringBuilder("server=localhost")
        kcsb2["AAD User ID"] = user
        kcsb2["Password"] = password
        kcsb2["aad federated security"] = True
        kcsbs.append(kcsb2)

        for kcsb in kcsbs:
            assert kcsb.data_source == "localhost"
            assert kcsb.aad_federated_security
            assert kcsb.aad_user_id == user
            assert kcsb.password == password
            assert kcsb.application_client_id is None
            assert kcsb.application_key is None
            assert kcsb.authority_id == "common"
            assert repr(kcsb) == "Data Source=localhost;AAD Federated Security=True;AAD User ID={0};Password={1};Authority Id=common".format(user, password)
            assert str(kcsb) == "Data Source=localhost;AAD Federated Security=True;AAD User ID={0};Password={1};Authority Id=common".format(
                user, self.PASSWORDS_REPLACEMENT
            )

    def test_aad_user_with_authority(self):
        """Checks kcsb that is created with AAD user credentials."""
        user = "test2"
        password = "Pa$$w0rd2"
        authority_id = "13456"

        kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication("localhost", user, password, authority_id)

        assert kcsb.data_source == "localhost"
        assert kcsb.aad_federated_security
        assert kcsb.aad_user_id == user
        assert kcsb.password == password
        assert kcsb.application_client_id is None
        assert kcsb.application_key is None
        assert kcsb.authority_id == authority_id
        assert repr(kcsb) == "Data Source=localhost;AAD Federated Security=True;AAD User ID={0};Password={1};Authority Id=13456".format(user, password)
        assert str(kcsb) == "Data Source=localhost;AAD Federated Security=True;AAD User ID={0};Password={1};Authority Id=13456".format(
            user, self.PASSWORDS_REPLACEMENT
        )

    def test_aad_device_login(self):
        """Checks kcsb that is created with AAD device login."""
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication("localhost")
        assert kcsb.data_source == "localhost"
        assert kcsb.aad_federated_security
        assert kcsb.aad_user_id is None
        assert kcsb.password is None
        assert kcsb.application_client_id is None
        assert kcsb.application_key is None
        assert kcsb.authority_id == "common"
        assert repr(kcsb) == "Data Source=localhost;AAD Federated Security=True;Authority Id=common"
        assert str(kcsb) == "Data Source=localhost;AAD Federated Security=True;Authority Id=common"

    def test_aad_app_token(self):
        """Checks kcsb that is created with AAD user token."""
        token = "The app hardest token ever"
        kcsb = KustoConnectionStringBuilder.with_aad_application_token_authentication("localhost", application_token=token)
        assert kcsb.data_source == "localhost"
        assert kcsb.application_token == token
        assert kcsb.aad_federated_security
        assert kcsb.aad_user_id is None
        assert kcsb.password is None
        assert kcsb.application_client_id is None
        assert kcsb.application_key is None
        assert kcsb.user_token is None
        assert kcsb.authority_id == "common"
        assert repr(kcsb) == "Data Source=localhost;AAD Federated Security=True;Authority Id=common;Application Token=%s" % token
        assert str(kcsb) == "Data Source=localhost;AAD Federated Security=True;Authority Id=common;Application Token=%s" % self.PASSWORDS_REPLACEMENT

    def test_aad_user_token(self):
        """Checks kcsb that is created with AAD user token."""
        token = "The user hardest token ever"
        kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication("localhost", user_token=token)
        assert kcsb.data_source == "localhost"
        assert kcsb.user_token == token
        assert kcsb.aad_federated_security
        assert kcsb.aad_user_id is None
        assert kcsb.password is None
        assert kcsb.application_client_id is None
        assert kcsb.application_key is None
        assert kcsb.application_token is None
        assert kcsb.authority_id == "common"
        assert repr(kcsb) == "Data Source=localhost;AAD Federated Security=True;Authority Id=common;User Token=%s" % token
        assert str(kcsb) == "Data Source=localhost;AAD Federated Security=True;Authority Id=common;User Token=%s" % self.PASSWORDS_REPLACEMENT

    def test_add_msi(self):
        client_guid = "kjhjk"
        object_guid = "87687687"
        res_guid = "kajsdghdijewhag"

        kcsb = [
            KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost0", timeout=1),
            KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost1", client_id=client_guid, timeout=2),
            KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost2", object_id=object_guid, timeout=3),
            KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost3", msi_res_id=res_guid),
        ]

        assert kcsb[0].msi_authentication
        assert kcsb[0].msi_parameters["resource"] == "localhost0"
        assert kcsb[0].msi_parameters["timeout"] == 1
        assert "client_id" not in kcsb[0].msi_parameters
        assert "object_id" not in kcsb[0].msi_parameters
        assert "msi_res_id" not in kcsb[0].msi_parameters

        assert kcsb[1].msi_authentication
        assert kcsb[1].msi_parameters["resource"] == "localhost1"
        assert kcsb[1].msi_parameters["timeout"] == 2
        assert kcsb[1].msi_parameters["client_id"] == client_guid
        assert "object_id" not in kcsb[1].msi_parameters
        assert "msi_res_id" not in kcsb[1].msi_parameters

        assert kcsb[2].msi_authentication
        assert kcsb[2].msi_parameters["resource"] == "localhost2"
        assert kcsb[2].msi_parameters["timeout"] == 3
        assert "client_id" not in kcsb[2].msi_parameters
        assert kcsb[2].msi_parameters["object_id"] == object_guid
        assert "msi_res_id" not in kcsb[2].msi_parameters

        assert kcsb[3].msi_authentication
        assert kcsb[3].msi_parameters["resource"] == "localhost3"
        assert "timeout" not in kcsb[3].msi_parameters
        assert "client_id" not in kcsb[3].msi_parameters
        assert "object_id" not in kcsb[3].msi_parameters
        assert kcsb[3].msi_parameters["msi_res_id"] == res_guid

        exception_occurred = False
        try:
            fault = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", client_id=client_guid, object_id=object_guid)
        except ValueError as e:
            exception_occurred = True
        finally:
            assert exception_occurred

    def test_add_token_provider(self):
        caller_token = "caller token"
        token_provider = lambda: caller_token
        kscb = KustoConnectionStringBuilder.with_token_provider("localhost", token_provider)

        assert kscb.token_provider() == caller_token

        exception_occurred = False
        try:
            kscb = KustoConnectionStringBuilder.with_token_provider("localhost", caller_token)
        except AssertionError as ex:
            exception_occurred = True
        finally:
            assert exception_occurred
