import asyncio
import copy
import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4
from xml.sax.saxutils import escape

import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from app.audit_moves import fingerprint
from app.upload_worker import Multipart, ProgressBody, WorkerError, error_code, error_details
from app.uploads import ProcessWorker


def client_error(code):
    return ClientError({'Error':{'Code':code}},'mock')


class S3:
    def __init__(self):
        self.object = None
        self.uploads = {}
        self.creates = self.completes = self.written_parts = 0
        self.aborts = []
        self.lost_complete = self.lost_create = False
        self.mutation = None
        self.metadata = {}

    def head_object(self, **kwargs):
        if self.object is None:raise client_error('404')
        return copy.deepcopy(self.object)

    def create_multipart_upload(self, **kwargs):
        self.creates += 1
        self.metadata = kwargs['Metadata']
        self.uploads['owned-'+str(self.creates)]={'key':kwargs['Key'],'parts':{}}
        if self.lost_create:raise ReadTimeoutError(endpoint_url='http://s3.test')
        return {'UploadId':'owned-'+str(self.creates)}

    def list_multipart_uploads(self, **kwargs):
        return {'Uploads':[{'Key':u['key'],'UploadId':identity} for identity,u in self.uploads.items()]}

    def list_parts(self, **kwargs):
        if kwargs['UploadId'] not in self.uploads:raise client_error('NoSuchUpload')
        return {'Parts':[{'PartNumber':n, 'Size':len(p), 'ETag':str(n)}
            for n,p in self.uploads[kwargs['UploadId']]['parts'].items()], 'IsTruncated':False}

    def upload_part(self, **kwargs):
        self.written_parts += 1
        body=kwargs['Body'].read()
        self.uploads[kwargs['UploadId']]['parts'][kwargs['PartNumber']]=body
        if self.mutation:self.mutation()
        return {'ETag':str(kwargs['PartNumber'])}

    def complete_multipart_upload(self, **kwargs):
        self.completes += 1
        parts=self.uploads.pop(kwargs['UploadId'])['parts']
        self.object={'ContentLength':sum(len(p) for p in parts.values()),'Metadata':self.metadata,'ETag':'verified-etag'}
        if self.lost_complete:raise ReadTimeoutError(endpoint_url='http://s3.test')
        return {'ETag':'verified-etag'}

    def abort_multipart_upload(self, **kwargs):
        self.aborts.append(kwargs['UploadId'])
        if kwargs['UploadId'] not in self.uploads:raise client_error('NoSuchUpload')
        del self.uploads[kwargs['UploadId']]


def item_for(tmp_path):
    path=tmp_path/'truck.mp4';path.write_bytes(b'whole-video-data')
    return {'id':uuid4().hex,'name':path.name,'path':str(path),'size':path.stat().st_size,
        'fileVersion':fingerprint(path.stat()),'object':{'bucket':'test','key':'unique-owner/truck.mp4'},'parts':{}}


def journal(item, events, stop_at=None):
    def emit(kind, critical=False, **values):
        events.append((kind,values))
        if kind=='creating':item['createAttempted']=True
        if kind=='created':item['uploadId']=values['uploadId']
        if kind=='part':item['parts'][str(values['number'])]=values
        if kind==stop_at:raise SystemExit('simulated restart')
    return emit


def test_lost_complete_response_is_verified_not_uploaded_or_deleted_twice(tmp_path):
    item=item_for(tmp_path);s3=S3();s3.lost_complete=True;events=[]
    Multipart(s3,item,journal(item,events)).run()
    assert events[-1][0]=='complete' and s3.creates==s3.completes==1
    Multipart(s3,item,journal(item,events)).run()
    assert s3.creates==s3.completes==1 and not s3.aborts


@pytest.mark.parametrize('stage',['creating','created','part','completing','complete'])
def test_restart_at_each_critical_stage_keeps_exact_multipart_and_object(tmp_path,stage):
    item=item_for(tmp_path);s3=S3();events=[]
    with pytest.raises(SystemExit):Multipart(s3,item,journal(item,events,stage)).run()
    Multipart(s3,item,journal(item,events)).run()
    assert events[-1][0]=='complete'
    assert s3.creates==s3.completes==1 and s3.written_parts==1 and not s3.aborts


def test_lost_create_response_discovers_exact_owned_key(tmp_path):
    item=item_for(tmp_path);s3=S3();s3.lost_create=True;events=[]
    with pytest.raises(ReadTimeoutError):Multipart(s3,item,journal(item,events)).run()
    assert item['createAttempted'] and not item.get('uploadId')
    s3.lost_create=False
    Multipart(s3,item,journal(item,events)).run()
    assert s3.creates==1 and events[-1][0]=='complete'


def test_ambiguous_create_never_adopts_or_aborts_any_upload(tmp_path):
    item=item_for(tmp_path);item['createAttempted']=True;s3=S3();events=[]
    s3.uploads={'a':{'key':item['object']['key'],'parts':{}},'b':{'key':item['object']['key'],'parts':{}}}
    with pytest.raises(WorkerError) as caught:Multipart(s3,item,journal(item,events)).run()
    assert caught.value.code=='multipart_unknown' and not s3.aborts and s3.creates==0


@pytest.mark.parametrize('field',['length','owner','version'])
def test_changed_complete_object_is_never_overwritten_or_cleaned(tmp_path,field):
    item=item_for(tmp_path);s3=S3();events=[]
    Multipart(s3,item,journal(item,events)).run()
    if field=='length':s3.object['ContentLength']+=1
    elif field=='owner':s3.object['Metadata']['uploader-id']='other-user'
    else:s3.object['Metadata']['file-version']='other-file'
    with pytest.raises(WorkerError) as caught:Multipart(s3,item,journal(item,events)).cleanup()
    assert caught.value.code=='object_changed' and not s3.aborts


def test_changed_source_stops_before_complete_and_keeps_other_uploads(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    s3.uploads['human']={'key':'human/video.mp4','parts':{1:b'human'}}
    s3.mutation=lambda:Path(item['path']).write_bytes(b'replaced video version')
    with pytest.raises(WorkerError) as caught:Multipart(s3,item,journal(item,events)).run()
    assert caught.value.code=='file_changed' and s3.completes==0
    Multipart(s3,item,journal(item,events)).cleanup()
    assert s3.aborts==[item['uploadId']] and 'human' in s3.uploads
    assert Path(item['path']).read_bytes()==b'replaced video version'


def test_unconfirmed_abort_remains_cleanup_pending(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    with pytest.raises(SystemExit):Multipart(s3,item,journal(item,events,'created')).run()
    s3.abort_multipart_upload=lambda **kwargs:None
    with pytest.raises(WorkerError) as caught:Multipart(s3,item,journal(item,events)).cleanup()
    assert caught.value.code=='cleanup_pending' and events[-1][0]!='cleaned'


def test_verified_move_can_resume_original_metadata_with_new_filesystem_version(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    with pytest.raises(SystemExit):Multipart(s3,item,journal(item,events,'part')).run()
    replacement=tmp_path/'other.mp4';replacement.write_bytes(Path(item['path']).read_bytes())
    item.update(path=str(replacement),sourceFileVersion=fingerprint(replacement.stat()))
    Multipart(s3,item,journal(item,events)).run()
    assert events[-1][0]=='complete' and s3.written_parts==1


def test_invalid_part_lengths_block_completion(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    with pytest.raises(SystemExit):Multipart(s3,item,journal(item,events,'created')).run()
    s3.uploads[item['uploadId']]['parts'][1]=b'too-short'
    with pytest.raises(WorkerError) as caught:Multipart(s3,item,journal(item,events)).run()
    assert caught.value.code=='parts_invalid' and s3.completes==0


def test_gateway_head_403_uses_bounded_get_to_prove_absence(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    s3.head_object=lambda **kwargs:(_ for _ in ()).throw(client_error('403'))
    def absent(**kwargs):
        assert kwargs['Range']=='bytes=0-0'
        raise client_error('NoSuchKey')
    s3.get_object=absent
    assert Multipart(s3,item,journal(item,events)).head() is None


def test_fresh_nonce_key_can_start_without_listing_but_retry_must_reconcile(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    item['object']['key']='owned/simple-'+item['id']+'/'+item['name']
    calls=[];head=s3.head_object
    def checked(**kwargs):
        calls.append('head')
        if not s3.creates:raise client_error('403')
        return head(**kwargs)
    s3.head_object=checked
    Multipart(s3,item,journal(item,events)).run()
    assert calls==['head'] and events[-1][0]=='complete'
    # Even without a returned UploadId, a recorded create intent must not write again blindly.
    item['createAttempted']=True;item.pop('uploadId',None)
    s3.head_object=lambda **kwargs:(_ for _ in ()).throw(client_error('403'))
    s3.get_object=lambda **kwargs:(_ for _ in ()).throw(client_error('AccessDenied'))
    with pytest.raises(WorkerError) as caught:Multipart(s3,item,journal(item,events)).run()
    assert caught.value.code=='multipart_unknown'
    assert s3.creates==1


def test_upload_only_grant_can_complete_with_bound_s3_receipt_without_read_or_list(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    item['object']['key']='owned/simple-'+item['id']+'/'+item['name']
    s3.head_object=lambda **kwargs:(_ for _ in ()).throw(client_error('403'))
    s3.get_object=lambda **kwargs:(_ for _ in ()).throw(client_error('AccessDenied'))
    s3.list_parts=lambda **kwargs:(_ for _ in ()).throw(client_error('AccessDenied'))
    finish=s3.complete_multipart_upload
    def complete(**kwargs):
        result=finish(**kwargs)
        return {**result,'Bucket':kwargs['Bucket'],'Key':kwargs['Key']}
    s3.complete_multipart_upload=complete
    Multipart(s3,item,journal(item,events)).run()
    assert events[-1][0]=='complete' and s3.creates==s3.completes==1


def test_upload_only_resume_uses_acknowledged_etags_and_never_creates_another_id(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    with pytest.raises(SystemExit):Multipart(s3,item,journal(item,events,'part')).run()
    s3.head_object=lambda **kwargs:(_ for _ in ()).throw(client_error('403'))
    s3.get_object=lambda **kwargs:(_ for _ in ()).throw(client_error('AccessDenied'))
    s3.list_parts=lambda **kwargs:(_ for _ in ()).throw(client_error('AccessDenied'))
    finish=s3.complete_multipart_upload
    def complete(**kwargs):
        return {**finish(**kwargs),'Bucket':kwargs['Bucket'],'Key':kwargs['Key']}
    s3.complete_multipart_upload=complete
    Multipart(s3,item,journal(item,events)).run()
    assert events[-1][0]=='complete' and s3.creates==1 and s3.written_parts==1


def test_gateway_bounded_get_preserves_full_size_and_metadata(tmp_path):
    import io
    item=item_for(tmp_path);s3=S3();events=[]
    Multipart(s3,item,journal(item,events)).run()
    original=copy.deepcopy(s3.object)
    s3.head_object=lambda **kwargs:(_ for _ in ()).throw(client_error('403'))
    body=io.BytesIO(b'x')
    def get(**kwargs):
        assert kwargs['Range']=='bytes=0-0'
        return {**original,'ContentLength':1,'ContentRange':f"bytes 0-0/{item['size']}",'Body':body}
    s3.get_object=get
    assert Multipart(s3,item,journal(item,events)).head()['size']==item['size']
    assert body.closed


def test_diagnostics_exclude_exception_message_headers_and_credentials():
    error=ClientError({'Error':{'Code':'AccessDenied','Message':'test-secret https://signed.invalid/?credential=secret'},
        'ResponseMetadata':{'HTTPStatusCode':403,'HTTPHeaders':{'Authorization':'test-secret'}}},'GetObject')
    details=error_details(error,{'id':'a'*32},'get_object')
    assert details=={'id':'a'*32,'stage':'get_object','type':'ClientError','serviceCode':'AccessDenied','httpStatus':403}
    assert 'test-secret' not in json.dumps(details) and error_code(client_error('403'))=='authorization'


def test_lost_completion_while_disconnected_preserves_network_error_and_journal(tmp_path):
    item=item_for(tmp_path);s3=S3();events=[]
    def disconnected(**kwargs):
        raise ReadTimeoutError(endpoint_url='http://fixture.invalid')
    s3.complete_multipart_upload=disconnected
    original=s3.head_object
    s3.head_object=lambda **kwargs:disconnected(**kwargs) if s3.creates else original(**kwargs)
    with pytest.raises(ReadTimeoutError) as caught:
        Multipart(s3,item,journal(item,events)).run()
    assert error_code(caught.value)=='network'
    assert error_details(caught.value,item,'head_object')['serviceCode']==''
    assert item['uploadId'] in s3.uploads and not s3.aborts


def test_checksum_reads_are_not_reported_as_uploaded_network_bytes():
    events=[]
    body=ProgressBody(b'abcdefgh',100,lambda kind,**values:events.append((kind,values)))
    body.read();body.seek(0)
    assert not events
    body.start_transfer();body.read(3)
    assert events==[('progress',{'bytes':103})]


class S3HTTP:
    """Local S3 wire fixture drives a real Boto3 child process, including XML/ETags."""
    def __init__(self):
        self.objects={};self.uploads={};self.requests=[]

    def handler(self):
        state=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def reply(self,status,body=b'',headers=None):
                self.send_response(status)
                for key,value in (headers or {}).items():self.send_header(key,value)
                self.send_header('Content-Length',str(len(body)));self.end_headers()
                if self.command!='HEAD':self.wfile.write(body)
            def handle_request(self):
                parsed=urlsplit(self.path);query=parse_qs(parsed.query,keep_blank_values=True)
                key=parsed.path
                state.requests.append((self.command,key,query))
                body=self.rfile.read(int(self.headers.get('Content-Length','0')))
                if self.command=='HEAD':
                    if key not in state.objects:self.reply(404);return
                    obj=state.objects[key]
                    self.send_response(200)
                    for k,v in obj['metadata'].items():self.send_header(k,v)
                    self.send_header('Content-Length',str(len(obj['body'])))
                    self.send_header('ETag','"wire-etag"');self.end_headers();return
                if self.command=='POST' and 'uploads' in query:
                    identity=uuid4().hex
                    state.uploads[identity]={'key':key,'parts':{},'metadata':{k:v for k,v in self.headers.items() if k.lower().startswith('x-amz-meta-')}}
                    self.reply(200,f'<InitiateMultipartUploadResult><Bucket>test</Bucket><Key>{escape(key)}</Key><UploadId>{identity}</UploadId></InitiateMultipartUploadResult>'.encode());return
                identity=query.get('uploadId',[''])[0]
                if identity not in state.uploads:self.reply(404,b'<Error><Code>NoSuchUpload</Code></Error>');return
                upload=state.uploads[identity]
                if self.command=='GET':
                    parts=''.join(f'<Part><PartNumber>{n}</PartNumber><ETag>"p{n}"</ETag><Size>{len(p)}</Size></Part>' for n,p in upload['parts'].items())
                    self.reply(200,f'<ListPartsResult><Bucket>test</Bucket><Key>{escape(key)}</Key><UploadId>{identity}</UploadId><IsTruncated>false</IsTruncated>{parts}</ListPartsResult>'.encode());return
                if self.command=='PUT':
                    number=int(query['partNumber'][0]);upload['parts'][number]=body
                    self.reply(200,headers={'ETag':f'"p{number}"'});return
                if self.command=='POST':
                    state.objects[key]={'body':b''.join(upload['parts'][n] for n in sorted(upload['parts'])),'metadata':upload['metadata']}
                    del state.uploads[identity]
                    self.reply(200,b'<CompleteMultipartUploadResult><Bucket>test</Bucket><Key>test</Key><ETag>"wire-etag"</ETag></CompleteMultipartUploadResult>');return
                if self.command=='DELETE':del state.uploads[identity];self.reply(204);return
                self.reply(400)
            do_HEAD=do_POST=do_GET=do_PUT=do_DELETE=handle_request
        return Handler


@pytest.mark.asyncio
async def test_real_isolated_process_boto3_protocol_and_resume_without_second_upload(tmp_path):
    state=S3HTTP();server=ThreadingHTTPServer(('127.0.0.1',0),state.handler())
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    worker=ProcessWorker();item=item_for(tmp_path);events=[]
    credentials={'endpoint':f'http://127.0.0.1:{server.server_port}','bucket':'test','accessKey':'test-key','secretKey':'test-secret'}
    async def callback(event):
        events.append(event)
        if event['event']=='created':item['uploadId']=event['uploadId']
        assert 'test-secret' not in json.dumps(event)
    try:
        await asyncio.wait_for(worker.run(item,credentials,callback),15)
        assert events[-1]['event']=='complete'
        assert next(iter(state.objects.values()))['body']==Path(item['path']).read_bytes()
        assert not state.uploads and not worker.processes
        writes=sum(r[0]=='PUT' for r in state.requests)
        await asyncio.wait_for(worker.run(item,credentials,callback),15)
        assert sum(r[0]=='PUT' for r in state.requests)==writes
        assert events[-1]['event']=='complete'
    finally:
        server.shutdown();server.server_close();thread.join(timeout=2)


@pytest.mark.asyncio
async def test_stopping_one_real_worker_then_aborting_only_its_id_leaves_other_worker_intact(tmp_path):
    state=S3HTTP();server=ThreadingHTTPServer(('127.0.0.1',0),state.handler())
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    worker=ProcessWorker()
    first_root=tmp_path/'one';first_root.mkdir();first=item_for(first_root)
    second_root=tmp_path/'two';second_root.mkdir();second=item_for(second_root)
    second['object']['key']='other-owner/truck.mp4'
    credentials={'endpoint':f'http://127.0.0.1:{server.server_port}','accessKey':'test-key','secretKey':'test-secret'}
    created=asyncio.Event();hang=asyncio.Event();events=[]
    async def first_event(event):
        if event['event']=='created':
            first['uploadId']=event['uploadId'];created.set();await hang.wait()
    async def second_event(event):events.append(event)
    task=asyncio.create_task(worker.run(first,credentials,first_event))
    try:
        await asyncio.wait_for(created.wait(),10)
        await asyncio.wait_for(worker.run(second,credentials,second_event),10)
        assert events[-1]['event']=='complete' and first['uploadId'] in state.uploads
        task.cancel();await asyncio.gather(task,return_exceptions=True)
        assert not worker.processes
        await asyncio.wait_for(worker.run(first,credentials,second_event,'cleanup'),10)
        assert events[-1]['event']=='cleaned' and not state.uploads
        assert list(state.objects)==['/test/other-owner/truck.mp4']
        assert Path(first['path']).is_file() and Path(second['path']).is_file()
    finally:
        task.cancel();await asyncio.gather(task,return_exceptions=True)
        server.shutdown();server.server_close();thread.join(timeout=2)
