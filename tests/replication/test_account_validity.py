# Copyright 2022 The Matrix.org Foundation C.I.C.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Optional

from twisted.internet.defer import ensureDeferred

import synapse
from synapse.module_api import DatabasePool, LoggingTransaction, ModuleApi

from tests.replication._base import BaseMultiWorkerStreamTestCase
from tests.server import make_request


class MockAccountValidity:
    def __init__(
        self,
        config,
        api: ModuleApi,
        reactor,
    ):
        self._api = api

        ensureDeferred(self.create_db())
        reactor.pump([0.0])

        self._api.register_account_validity_callbacks(
            is_user_expired=self.is_user_expired,
            on_user_registration=self.on_user_registration,
        )

    async def create_db(self):
        def create_table_txn(txn: LoggingTransaction):
            txn.execute(
                """
                CREATE TABLE IF NOT EXISTS mock_account_validity(
                    user_id TEXT PRIMARY KEY,
                    expired BOOLEAN NOT NULL
                )
                """,
                (),
            )

        await self._api.run_db_interaction(
            "account_validity_create_table",
            create_table_txn,
        )

    async def is_user_expired(self, user_id: str) -> Optional[bool]:
        def get_expiration_for_user_txn(txn: LoggingTransaction):
            return DatabasePool.simple_select_one_onecol_txn(
                txn=txn,
                table="mock_account_validity",
                keyvalues={"user_id": user_id},
                retcol="expired",
                allow_none=True,
            )

        return await self._api.run_db_interaction(
            "get_expiration_for_user",
            get_expiration_for_user_txn,
        )

    async def on_user_registration(self, user_id: str) -> None:
        def add_valid_user_txn(txn: LoggingTransaction):
            txn.execute(
                "INSERT INTO mock_account_validity (user_id, expired) VALUES (?, ?)",
                (user_id, False),
            )

        await self._api.run_db_interaction(
            "account_validity_add_valid_user",
            add_valid_user_txn,
        )

    async def expire_user(self, user_id: str) -> None:
        def set_expired_user_txn(txn: LoggingTransaction):
            txn.execute(
                "UPDATE mock_account_validity SET expired = 1 WHERE user_id = ?",
                (user_id,),
            )

        await self._api.run_db_interaction(
            "account_validity_set_expired_user",
            set_expired_user_txn,
        )


class WorkerAccountValidityTestCase(BaseMultiWorkerStreamTestCase):
    servlets = [
        synapse.rest.admin.register_servlets,
        synapse.rest.client.account.register_servlets,
        synapse.rest.client.login.register_servlets,
        synapse.rest.client.register.register_servlets,
    ]

    def make_homeserver(self, reactor, clock):
        hs_config = self.default_config()
        hs_config["modules"] = [
            {
                "module": __name__ + ".MockAccountValidity",
            }
        ]

        hs = self.setup_test_homeserver(config=hs_config)

        module_api = hs.get_module_api()
        for module, config in hs.config.modules.loaded_modules:
            self.module = module(config=config, api=module_api, reactor=self.reactor)

        return hs

    def _create_user_and_expire(self) -> str:
        self.register_user("user", "pass")
        user_id = "@user:test"
        token = self.login("user", "pass")

        channel = self.make_request(
            "GET",
            "/_matrix/client/v3/account/whoami",
            access_token=token,
        )

        self.assertEqual(channel.code, 200)
        self.assertEqual(channel.json_body["user_id"], user_id)

        self.get_success_or_raise(self.module.expire_user(user_id))

        return token

    def test_account_validity(self):
        token = self._create_user_and_expire()

        channel = self.make_request(
            "GET",
            "/_matrix/client/v3/account/whoami",
            access_token=token,
        )

        self.assertEqual(channel.code, 403)

    def test_account_validity_with_worker(self):
        worker_hs = self.make_worker_hs("synapse.app.generic_worker")
        worker_site = self._hs_to_site[worker_hs]

        token = self._create_user_and_expire()

        channel = make_request(
            self.reactor,
            worker_site,
            "GET",
            "/_matrix/client/v3/account/whoami",
            access_token=token,
        )
        self.assertEqual(channel.code, 403)
