"""Exact-object recovery tests; all grants, servers and source files are synthetic."""
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from app.upload_storage import BoundStorage, StorageBindingError, runtime_management_credentials
from app.upload_worker import ProgressBody, error_code, error_details


def rejected(code='AccessDenied'):
    return ClientError({'Error': {'Code': code}}, 'fixture')


class Denied:
    def __getattr__(self, operation):
        def request(**kwargs):
            raise rejected()
        return request


class Objects:
    def __init__(self):
        self.calls = []
        self.exists = False
        self.uploads = []
        self.truncated = False

    def __getattr__(self, operation):
        def request(**kwargs):
            self.calls.append((operation, kwargs))
            if operation == 'head_object':
                if not self.exists:
                    raise rejected('NoSuchKey')
                return {'ContentLength': 12}
            if operation == 'list_multipart_uploads':
                return {'Uploads': self.uploads, 'IsTruncated': self.truncated}
            if operation == 'create_multipart_upload':
                return {'UploadId': 'created-owned'}
            if operation == 'list_parts':
                return {'Parts': [{'PartNumber': 1, 'Size': 12, 'ETag': 'confirmed'}]}
            return {'ETag': 'receipt'}
        return request


def binding():
    identity = 'a' * 32
    return {'id': identity, 'dataset': '27912', 'name': 'truck.mp4',
            'fileVersion': {'device': 1, 'inode': 2, 'size': 12, 'modifiedNs': 3},
            'object': {'bucket': 'fixture', 'key': f'old/simple-{identity}/truck.mp4'}}


def storage(item=None, primary=None):
    item = item or binding()
    recovery = Objects()
    factories = []
    def factory():
        factories.append(True)
        return recovery
    client = BoundStorage(primary or Denied(), item, {'prefix': 'renewed/'}, factory)
    return client, recovery, factories


def test_recovery_resumes_only_recorded_multipart_and_leaves_other_ids_untouched():
    item = binding(); item['uploadId'] = 'owned'
    client, remote, factories = storage(item)
    base = dict(Bucket='fixture', Key=item['object']['key'], UploadId='owned')
    assert client.list_parts(**base)['Parts'][0]['ETag'] == 'confirmed'
    client.upload_part(**base, PartNumber=2, Body=b'rest')
    client.complete_multipart_upload(**base, MultipartUpload={'Parts': []})
    assert len(factories) == 1
    with pytest.raises(StorageBindingError):
        client.abort_multipart_upload(**{**base, 'UploadId': 'human'})
    assert [x[0] for x in remote.calls] == ['list_parts', 'upload_part', 'complete_multipart_upload']


@pytest.mark.parametrize('arguments', [
    {'Bucket': 'another', 'Key': 'anything'},
    {'Bucket': 'fixture', 'Key': 'another'},
    {'Bucket': 'fixture', 'Prefix': ''},
])
def test_binding_rejected_before_either_client_or_factory(arguments):
    client, remote, factories = storage()
    with pytest.raises(StorageBindingError):
        if 'Prefix' in arguments:
            client.list_multipart_uploads(**arguments)
        else:
            client.head_object(**arguments)
    assert not factories and not remote.calls
    for operation in ('delete_object', 'list_objects_v2', 'delete_bucket'):
        with pytest.raises(AttributeError):
            getattr(client, operation)


def test_discovery_cannot_authorize_another_objects_multipart_id():
    item = binding(); client, remote, _ = storage(item)
    remote.uploads = [{'Key': item['object']['key'], 'UploadId': 'owned'},
                      {'Key': item['object']['key'] + '-other', 'UploadId': 'human'}]
    client.list_multipart_uploads(Bucket='fixture', Prefix=item['object']['key'])
    client.abort_multipart_upload(Bucket='fixture', Key=item['object']['key'], UploadId='owned')
    with pytest.raises(StorageBindingError):
        client.abort_multipart_upload(Bucket='fixture', Key=item['object']['key'], UploadId='human')


def create_arguments(item):
    marker = hashlib.sha256(json.dumps(item['fileVersion'], sort_keys=True).encode()).hexdigest()
    return dict(Bucket='fixture', Key=item['object']['key'],
                Metadata={'uploader-id': item['id'], 'file-version': marker})


def test_old_namespace_creation_requires_absence_and_keeps_exact_key():
    item = binding(); client, remote, _ = storage(item)
    args = create_arguments(item)
    assert client.create_multipart_upload(**args)['UploadId'] == 'created-owned'
    client.list_parts(Bucket='fixture', Key=item['object']['key'], UploadId='created-owned')
    assert [c[0] for c in remote.calls] == ['head_object', 'list_multipart_uploads', 'create_multipart_upload', 'list_parts']
    assert all(c[1].get('Key', c[1].get('Prefix')) == item['object']['key'] for c in remote.calls)


@pytest.mark.parametrize('obstacle', ['complete', 'multipart', 'truncated', 'metadata', 'current_grant'])
def test_old_namespace_recovery_never_overwrites_or_creates_on_uncertain_identity(obstacle):
    item = binding(); client, remote, _ = storage(item)
    args = create_arguments(item)
    if obstacle == 'complete': remote.exists = True
    if obstacle == 'multipart': remote.uploads = [{'Key': item['object']['key'], 'UploadId': 'unconfirmed'}]
    if obstacle == 'truncated': remote.truncated = True
    if obstacle == 'metadata': args['Metadata']['uploader-id'] = 'another'
    if obstacle == 'current_grant': client.credentials['prefix'] = 'old/'
    with pytest.raises((StorageBindingError, ClientError)):
        client.create_multipart_upload(**args)
    assert not any(c[0] == 'create_multipart_upload' for c in remote.calls)


def test_network_errors_do_not_trigger_management_fallback_and_pause_without_failure():
    def unavailable(**kwargs):
        raise ReadTimeoutError(endpoint_url='http://fixture.invalid')
    item = binding(); client, _, factories = storage(item, SimpleNamespace(head_object=unavailable))
    with pytest.raises(ReadTimeoutError):
        client.head_object(Bucket='fixture', Key=item['object']['key'])
    assert not factories
    assert error_code(httpx.ConnectError('fixture-offline')) == 'network'


def test_denied_put_rewinds_body_and_recovery_does_not_retry_rejected_grant():
    item = binding(); item['uploadId'] = 'owned'
    rejected_bodies, recovered_bodies = [], []
    def reject(**kwargs):
        kwargs['Body'].start_transfer()
        rejected_bodies.append(kwargs['Body'].read())
        raise rejected()
    client, remote, _ = storage(item, SimpleNamespace(upload_part=reject))
    def accept(**kwargs):
        assert not kwargs['Body'].reporting
        recovered_bodies.append(kwargs['Body'].read())  # signing/checksum preparation
        kwargs['Body'].seek(0); kwargs['Body'].start_transfer()
        assert kwargs['Body'].read() == b'complete-part'
        return {'ETag': 'confirmed'}
    remote.upload_part = accept
    base = dict(Bucket='fixture', Key=item['object']['key'], UploadId='owned', PartNumber=1)
    client.upload_part(**base, Body=ProgressBody(b'complete-part', 0, lambda *a, **kw: None))
    client.upload_part(**base, Body=ProgressBody(b'complete-part', 0, lambda *a, **kw: None))
    assert rejected_bodies == [b'complete-part'] and recovered_bodies == [b'complete-part'] * 2


def test_runtime_configuration_is_memory_only_and_fixed_host(monkeypatch):
    calls = []
    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(200, json={'MINIO_USERNAME': 'fixture-access', 'MINIO_PASSWORD': 'fixture-secret'})
    original = httpx.Client
    monkeypatch.setattr(httpx, 'Client', lambda **options: original(**options, transport=httpx.MockTransport(respond)))
    credentials = {'endpoint': 'http://pre-athena-mv-fss.cisdigital.cn', 'region': 'local'}
    values = runtime_management_credentials(credentials, binding())
    assert values == {**credentials, 'accessKey': 'fixture-access', 'secretKey': 'fixture-secret'}
    assert calls == ['http://pre-athena-mv.cisdigital.cn/conf/config.json']
    with pytest.raises(StorageBindingError):
        runtime_management_credentials({'endpoint': 'http://different.invalid'}, binding())
    assert len(calls) == 1


def test_service_error_codes_cannot_carry_arbitrary_tokens_into_diagnostics():
    error = rejected('fixture-sensitive-token')
    assert error_details(error, binding(), 'head_object')['serviceCode'] == ''
