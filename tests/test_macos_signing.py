import json
from types import SimpleNamespace

import pytest

from scripts import macos_signing, release_assets


IDENTITY = macos_signing.DeveloperIdentity("A" * 40, "ABCDE12345")
JOB = "ee9300c9-7b1d-4bbd-a278-8c95a03341ef"
IMAGE_HASH, CODE_HASH = "1" * 40, "2" * 40


def test_only_developer_id_application_is_selected(monkeypatch):
    listing = (
        f'  1) {IDENTITY.certificate} "Developer ID Application: Release Team ({IDENTITY.team})"\n'
        f'  2) {"B" * 40} "Apple Development: Local Test ({IDENTITY.team})"\n'
        "     2 valid identities found\n"
    )
    monkeypatch.setattr(macos_signing, "tool", lambda *args, **kwargs: SimpleNamespace(stdout=listing))
    assert macos_signing.find_identity(IDENTITY.certificate.lower()) == IDENTITY
    with pytest.raises(ValueError, match="Developer ID Application"):
        macos_signing.find_identity("B" * 40)


def test_signed_distribution_requires_a_matching_team_and_notary_profile(monkeypatch):
    monkeypatch.setattr(release_assets, "find_identity", lambda _: IDENTITY)
    assert release_assets.distribution_identity({}, None, None) is None
    signed = {"signing": {"mode": "developer-id", "team": IDENTITY.team}}
    assert release_assets.distribution_identity(signed, "certificate", "release") == IDENTITY
    with pytest.raises(ValueError, match="--notary-profile"):
        release_assets.distribution_identity(signed, "certificate", None)
    with pytest.raises(ValueError, match="同一团队"):
        release_assets.distribution_identity({}, "certificate", "release")


@pytest.mark.parametrize(("status", "ticket", "error"), [
    ("Invalid", [], "公证未通过"),
    ("Accepted", [IMAGE_HASH], "未覆盖全部"),
    ("Accepted", [IMAGE_HASH, CODE_HASH], None),
])
def test_only_complete_accepted_ticket_is_stapled(monkeypatch, tmp_path, status, ticket, error):
    calls = []

    def tool(*args, **kwargs):
        calls.append(args)
        if "submit" in args:
            return SimpleNamespace(stdout=json.dumps({"id": JOB, "status": status}), returncode=0)
        if "log" in args:
            report = {"status": status, "jobId": JOB, "ticketContents": [{"cdhash": value} for value in ticket]}
            return SimpleNamespace(stdout=json.dumps(report))

    monkeypatch.setattr(macos_signing, "tool", tool)
    monkeypatch.setattr(macos_signing, "verify_developer_code", lambda *_: IMAGE_HASH)
    monkeypatch.setattr(macos_signing, "verify_notarized_image", lambda path: calls.append(("verify", path)))
    image = tmp_path / "release.dmg"
    args = (image, IDENTITY, "release")
    kwargs = {"identifier": "local.test.disk-image", "code_hashes": {CODE_HASH}}
    if error:
        with pytest.raises((ValueError, RuntimeError), match=error):
            macos_signing.notarize_image(*args, **kwargs)
        assert not any("staple" in call for call in calls)
    else:
        macos_signing.notarize_image(*args, **kwargs)
        assert calls[-2:] == [
            ("/usr/bin/xcrun", "stapler", "staple", image),
            ("verify", image),
        ]
