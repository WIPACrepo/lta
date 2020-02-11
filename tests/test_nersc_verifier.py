# test_nersc_verifier.py
"""Unit tests for lta/nersc_verifier.py."""

from unittest.mock import call, MagicMock

import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore

from lta.nersc_verifier import as_catalog_record, main, NerscVerifier
from .test_util import AsyncMock, ObjectLiteral

@pytest.fixture
def config():
    """Supply a stock NerscVerifier component configuration."""
    return {
        "COMPONENT_NAME": "testing-nersc_verifier",
        "FILE_CATALOG_REST_TOKEN": "fake-file-catalog-token",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "TAPE_BASE_PATH": "/path/to/hpss",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }

def test_as_catalog_record():
    """Check that as_catalog_record trims down metadata appropriately."""
    bundle_record = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/b33686fe41b711ea85fbc6259865d176.zip",
        "checksum": {
            "adler32": "5d69e3ca",
            "sha512": "e7ea63cf36d5b793c9c372d815b7781943cd9c2e2f664d0000de4595779036174902428cedcc32c9542260df620ff5a4cbee91fe124502bc9c71837233169606"
        },
        "claim_timestamp": "2020-02-11T04:05:04",
        "claimant": "cori05-nersc-verifier-5568d5d5-81a5-44a0-ac97-6f067e8d11a3",
        "claimed": False,
        "create_timestamp": "2020-01-28T10:19:42",
        "dest": "NERSC",
        "files": [
            {
                "checksum": {
                    "sha512": "b098ec20b12ee795dff25158873f86df1090f66479e3604b40dbce096a67f52f79f8decd2d1834a3b1e45dc42b703673b0ba194b93c5b705534d1a2422ce1cb8"
                },
                "file_size": 1188095831,
                "logical_name": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012/ukey_246d4979-e8b7-4fcf-96e5-4afa15a91d44_PFRaw_PhysicsFiltering_Run00131614_Subrun00000000_00000202.tar.gz",
                "meta_modify_date": "2020-01-28 10:15:26.125535",
                "uuid": "1a1e9648-41b7-11ea-85e1-666154400f62"
            },
            {
                "checksum": {
                    "sha512": "29532b62b8dcc0bdf0937ae7dc891be3ab57ded69913d27ab552051fc36d9fe04573ef8a705e6c81c344c0888af0c0de1664c6e6a8fa830d72362b87c5c60ec6"
                },
                "file_size": 1186829262,
                "logical_name": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012/ukey_4ba6dab2-eacb-4bec-a80b-27eb5c54893d_PFRaw_PhysicsFiltering_Run00131613_Subrun00000000_00000185.tar.gz",
                "meta_modify_date": "2020-01-28 10:13:47.437259",
                "uuid": "df4bf342-41b6-11ea-9608-666154400f62"
            },
            {
                "checksum": {
                    "sha512": "78dd0c99dd413ce234b689b0752cf01134a798c4971da59321b34cffb0be58b01099f97a3e8cba714183d3f4aa50a3559541975bc8c4e05141ff131b12d3cd08"
                },
                "file_size": 1186640189,
                "logical_name": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012/ukey_44c00140-770f-4655-8d60-fb75997e8ef9_PFRaw_PhysicsFiltering_Run00131613_Subrun00000000_00000204.tar.gz",
                "meta_modify_date": "2020-01-28 10:13:22.664175",
                "uuid": "d087dff6-41b6-11ea-b2e4-666154400f62"
            },
            {
                "checksum": {
                    "sha512": "33ace6b9b780f97bcf18382464fe08c96bcdb3a4aef5d6ac7656cdbbe05e75a73e95fd8b64207604484c04173ac566b96051d853749a67da26693b85ddd5bc09"
                },
                "file_size": 1186616485,
                "logical_name": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012/ukey_b2a04d2b-904d-4271-98cf-1ceff5947318_PFRaw_PhysicsFiltering_Run00131613_Subrun00000000_00000148.tar.gz",
                "meta_modify_date": "2020-01-28 10:15:56.761259",
                "uuid": "2c6139f8-41b7-11ea-bd61-666154400f62"
            },
            {
                "checksum": {
                    "sha512": "34c3ea6508cb2123c2cdbe70a64fd37f983bef13a32a00afc92a4df808ebeea5b8176ba2232fe7da48fb1c4986c3263dc4df6e00c5c51c3f44821e0018cb99b3"
                },
                "file_size": 1186547342,
                "logical_name": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012/ukey_6d48effb-ce8e-48ce-8c17-3e05029eee09_PFRaw_PhysicsFiltering_Run00131615_Subrun00000000_00000034.tar.gz",
                "meta_modify_date": "2020-01-28 10:14:17.251869",
                "uuid": "f1114e3a-41b6-11ea-bee2-666154400f62"
            },
            {
                "checksum": {
                    "sha512": "af834a9d03ee6390610d8bf8ddc46219369a505d0a42b9035eecbf92c3f34db966736392b8b02d1932ddf12ca2828cb5e43de9903af9945bf686d8de135f9c0d"
                },
                "file_size": 1186445818,
                "logical_name": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012/ukey_2f6786cd-245b-4e8d-815b-7f7f3ae9b313_PFRaw_PhysicsFiltering_Run00131612_Subrun00000000_00000050.tar.gz",
                "meta_modify_date": "2020-01-28 10:14:49.604150",
                "uuid": "0459dd3a-41b7-11ea-9bd9-666154400f62"
            }
        ],
        "path": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012",
        "reason": "",
        "request": "945e546841b711eaaf1bc6259865d176",
        "size": 7121179847,
        "source": "WIPAC",
        "status": "completed",
        "type": "Bundle",
        "update_timestamp": "2020-02-11T04:06:03",
        "uuid": "b33686fe41b711ea85fbc6259865d176",
        "verified": False
    }
    assert as_catalog_record(bundle_record) == {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/b33686fe41b711ea85fbc6259865d176.zip",
        "checksum": {
            "adler32": "5d69e3ca",
            "sha512": "e7ea63cf36d5b793c9c372d815b7781943cd9c2e2f664d0000de4595779036174902428cedcc32c9542260df620ff5a4cbee91fe124502bc9c71837233169606"
        },
        "claim_timestamp": "2020-02-11T04:05:04",
        "claimant": "cori05-nersc-verifier-5568d5d5-81a5-44a0-ac97-6f067e8d11a3",
        "claimed": False,
        "create_timestamp": "2020-01-28T10:19:42",
        "dest": "NERSC",
        "files": [
            "1a1e9648-41b7-11ea-85e1-666154400f62",
            "df4bf342-41b6-11ea-9608-666154400f62",
            "d087dff6-41b6-11ea-b2e4-666154400f62",
            "2c6139f8-41b7-11ea-bd61-666154400f62",
            "f1114e3a-41b6-11ea-bee2-666154400f62",
            "0459dd3a-41b7-11ea-9bd9-666154400f62",
        ],
        "path": "/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1012",
        "reason": "",
        "request": "945e546841b711eaaf1bc6259865d176",
        "size": 7121179847,
        "source": "WIPAC",
        "status": "completed",
        "type": "Bundle",
        "update_timestamp": "2020-02-11T04:06:03",
        "uuid": "b33686fe41b711ea85fbc6259865d176",
        "verified": False
    }

def test_constructor_config(config, mocker):
    """Test that a NerscVerifier can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    assert p.name == "testing-nersc_verifier"
    assert p.file_catalog_rest_token == "fake-file-catalog-token"
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_patch_retries == 3
    assert p.heartbeat_patch_timeout_seconds == 30
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_token == "fake-lta-rest-token"
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.source_site == "WIPAC"
    assert p.tape_base_path == "/path/to/hpss"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock

def test_do_status(config, mocker):
    """Verify that the NerscVerifier has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    assert p._do_status() == {}

@pytest.mark.asyncio
async def test_nersc_verifier_logs_configuration(mocker):
    """Test to make sure the NerscVerifier logs its configuration."""
    logger_mock = mocker.MagicMock()
    nersc_verifier_config = {
        "COMPONENT_NAME": "logme-testing-nersc_verifier",
        "FILE_CATALOG_REST_TOKEN": "logme-fake-file-catalog-token",
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "TAPE_BASE_PATH": "/logme/path/to/hpss",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    NerscVerifier(nersc_verifier_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("nersc_verifier 'logme-testing-nersc_verifier' is configured:"),
        call('COMPONENT_NAME = logme-testing-nersc_verifier'),
        call('FILE_CATALOG_REST_TOKEN = logme-fake-file-catalog-token'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('RUN_ONCE_AND_DIE = False'),
        call('SOURCE_SITE = WIPAC'),
        call('TAPE_BASE_PATH = /logme/path/to/hpss'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)

@pytest.mark.asyncio
async def test_script_main(config, mocker, monkeypatch):
    """
    Verify NerscVerifier component behavior when run as a script.

    Test to make sure running the NerscVerifier as a script does the setup work
    that we expect and then launches the nersc_verifier service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.nersc_verifier.status_loop")
    mock_work_loop = mocker.patch("lta.nersc_verifier.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()

@pytest.mark.asyncio
async def test_nersc_verifier_run(config, mocker):
    """Test the NerscVerifier does the work the nersc_verifier should do."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()

@pytest.mark.asyncio
async def test_nersc_verifier_run_exception(config, mocker):
    """Test an error doesn't kill the NerscVerifier."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp

@pytest.mark.asyncio
async def test_nersc_verifier_hpss_not_available(config, mocker):
    """Test that a bad returncode on hpss_avail will prevent work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=1,
        args=["/usr/common/mss/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    p = NerscVerifier(config, logger_mock)
    assert not await p._do_work_claim()

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_pop_exception(config, mocker):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = NerscVerifier(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_yes_results(config, mocker):
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = NerscVerifier(config, logger_mock)
    await p._do_work()
    assert dwc_mock.call_count == 3

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_no_result(config, mocker):
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": None
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    p = NerscVerifier(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_not_called()

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_yes_result(config, mocker):
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    vbih_mock.return_value = False
    p = NerscVerifier(config, logger_mock)
    assert await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_called_with(mocker.ANY, {"one": 1})

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_yes_result_update_fc_and_lta(config, mocker):
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    vbih_mock.return_value = True
    abtfc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._add_bundle_to_file_catalog", new_callable=AsyncMock)
    ubild_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._update_bundle_in_lta_db", new_callable=AsyncMock)
    p = NerscVerifier(config, logger_mock)
    assert await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_called_with(mocker.ANY, {"one": 1})
    abtfc_mock.assert_called_with({"one": 1})
    ubild_mock.assert_called_with(mocker.ANY, {"one": 1})

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_exception_caught(config, mocker):
    """Test that _do_work_claim quarantines a Bundle if it catches an Exception."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = [
        {
            "bundle": {
                "uuid": "45ae2ad39c664fda86e5981be0976d9c",
                "one": 1,
            },
        },
        {}
    ]
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    vbih_mock.side_effect = Exception("Database totally on fire, guys")
    p = NerscVerifier(config, logger_mock)
    assert not await p._do_work_claim()
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/45ae2ad39c664fda86e5981be0976d9c', mocker.ANY)
    vbih_mock.assert_called_with(mocker.ANY, {"uuid": "45ae2ad39c664fda86e5981be0976d9c", "one": 1})

@pytest.mark.asyncio
async def test_nersc_verifier_add_bundle_to_file_catalog(config, mocker):
    """Test that _add_bundle_to_file_catalog adds a record for the bundle and adds its location to constituent files."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "size": 12345,
        "files": [
            {"uuid": "e0d15152-fd73-4e98-9aea-a9e5fdd8618e"},
            {"uuid": "e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4"},
            {"uuid": "93bcd96e-0110-4064-9a79-b5bdfa3effb4"},
        ]
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.side_effect = [
        True,  # POST /api/files - create the bundle record
        {  # GET /api/files/UUID - get the file record
            "uuid": "e0d15152-fd73-4e98-9aea-a9e5fdd8618e",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file1.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
        {  # GET /api/files/UUID - get the file record
            "uuid": "e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file2.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
        {  # GET /api/files/UUID - get the file record
            "uuid": "93bcd96e-0110-4064-9a79-b5bdfa3effb4",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file3.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
    ]
    p = NerscVerifier(config, logger_mock)
    assert await p._add_bundle_to_file_catalog(bundle)
    assert fc_rc_mock.call_count == 7
    fc_rc_mock.assert_called_with("POST", '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4/locations', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_update_bundle_in_lta_db(config, mocker):
    """Test that _update_bundle_in_lta_db updates the bundle as verified in the LTA DB."""
    logger_mock = mocker.MagicMock()
    bundle = {"uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef"}
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = True
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert await p._update_bundle_in_lta_db(lta_mock, bundle)
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_success_no_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha512) OK\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_not_called()

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hsi_failure_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss quarantines a bundle if the HSI command fails."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=1,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 1
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_mismatch_checksum_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss quarantines a bundle if the checksums do not match."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"1693e9d0273e3a2995b917c0e72e6bd2f40ea677f3613b6d57eaa14bd3a285c73e8db8b6e556b886c3929afe324bcc718711f2faddfeb43c3e030d9afe697873 sha512 /home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip [hsi]\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 1
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_failure_hashverify_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=1,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hashverify_bad_type_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha256) OK\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hashverify_bad_result_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha256) OK\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)
