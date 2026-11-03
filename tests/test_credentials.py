import json
import pytest
from app import credentials

@pytest.fixture
def keychain(monkeypatch,tmp_path):
    saved={}
    monkeypatch.setattr(credentials.secure_store,'save_payload',lambda key,value:saved.update({key:value}))
    monkeypatch.setattr(credentials.secure_store,'load_payload',lambda key:saved.get(key))
    monkeypatch.setattr(credentials,'CONFIG_DIR',tmp_path)
    monkeypatch.setattr(credentials,'CONFIG_FILE',tmp_path/'credentials.json')
    return saved

def test_credentials_round_trip_uses_keychain_without_plaintext_file(keychain):
    credentials.save('target','worker','example-secret')
    assert credentials.load('target')=={'account':'worker','password':'example-secret'}
    assert not credentials.CONFIG_FILE.exists()

def test_legacy_secret_removed_only_after_keychain_readback(keychain):
    credentials.CONFIG_FILE.write_text(json.dumps({'target':{'account':'worker','password':'example-secret'}}))
    assert credentials.load('target')['account']=='worker'
    assert not credentials.CONFIG_FILE.exists()

def test_failed_keychain_keeps_legacy_secret(keychain,monkeypatch):
    value=json.dumps({'target':{'account':'worker','password':'example-secret'}})
    credentials.CONFIG_FILE.write_text(value)
    monkeypatch.setattr(credentials.secure_store,'load_payload',lambda key:None)
    with pytest.raises(credentials.secure_store.SecureStoreError):credentials.load('target')
    assert credentials.CONFIG_FILE.read_text()==value

def test_credentials_reject_invalid_saved_payload(keychain):
    credentials.CONFIG_FILE.write_text('not-json')
    assert credentials.load('target') is None

def test_credentials_only_allow_known_services():
    with pytest.raises(ValueError):credentials.load('unknown')
