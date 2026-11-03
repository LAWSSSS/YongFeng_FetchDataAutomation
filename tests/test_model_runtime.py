import hashlib
import json
import shutil
import sys
import types
from pathlib import Path

import pytest

from app import slicing_worker as worker
from app.main import create_app
from app.slicing import source_version
from scripts.cache_runtime_models import cache
from tests.test_slicing_performance import close_app


def request_for(tmp_path):
    path=tmp_path/'video.mp4';path.write_bytes(b'immutable-video')
    return {'filePath':str(path),'frameToolPath':str(tmp_path/'ffmpeg'),
            'expectedFileVersion':source_version(path),'settings':{},'points':{'bed':[0]},
            'metadata':{'size':path.stat().st_size,'duration':60.,'width':3840,'height':2160,'frameRate':25.},
            'verifiedMedia':True}


def stub_worker(monkeypatch,request):
    class Models:
        def __init__(self,*_):pass
        def load(self,*_):pass
        def classify(self,*_):return {'label':'clear','score':0.1}
    Path(request['frameToolPath']).write_bytes(b'tool')
    monkeypatch.setattr(worker.os,'access',lambda *_:True)
    monkeypatch.setattr(worker,'Models',Models)
    monkeypatch.setattr(worker,'extract_requested_frames',lambda *a,**k:iter([(0,b'jpg')]))
    events=[];monkeypatch.setattr(worker,'emit',events.append)
    return events


def test_verified_exact_metadata_is_reused_without_duplicate_probe_or_tail(tmp_path,monkeypatch):
    request=request_for(tmp_path);events=stub_worker(monkeypatch,request)
    def forbidden(*_):raise AssertionError('No repeated metadata or tail scan for an exact verified version')
    monkeypatch.setattr(worker,'probe',forbidden);monkeypatch.setattr(worker,'verify_video_tail',forbidden)
    worker.process(request)
    assert any(e.get('event')=='observation' for e in events)
    request['expectedFileVersion']['modifiedNs']+=1
    with pytest.raises(worker.FrameExtractionError,match='source_changed'):worker.process(request)


def test_verified_flag_without_bound_metadata_still_checks_the_video(tmp_path,monkeypatch):
    request=request_for(tmp_path);metadata=request.pop('metadata');events=stub_worker(monkeypatch,request)
    calls=[]
    monkeypatch.setattr(worker,'probe',lambda *_:metadata)
    monkeypatch.setattr(worker,'verify_video_tail',lambda *_:calls.append(True))
    worker.process(request)
    assert calls and events[-1]['event']=='complete'


def test_each_magnet_process_has_bounded_runtime_threads(tmp_path,monkeypatch):
    model=tmp_path/'best.onnx';model.write_bytes(b'fixture')
    (tmp_path/'infer.py').write_text('def predict(*args): return [], []\n')
    captured=[]
    def session(path,*,sess_options,providers):
        captured.append((sess_options.intra_op_num_threads,sess_options.inter_op_num_threads,providers))
        return object()
    monkeypatch.setitem(sys.modules,'onnxruntime',types.SimpleNamespace(SessionOptions=types.SimpleNamespace,InferenceSession=session))
    worker.Models({'magnetDeployDirectory':str(tmp_path),'magnetModelPath':str(model)}, {'magnet'}).load('magnet')
    assert captured==[(2,1,['CPUExecutionProvider'])]


@pytest.mark.asyncio
async def test_model_cache_preserves_revision_and_requires_identical_resources(tmp_path,monkeypatch):
    app=create_app(data_dir=tmp_path/'db');m=app.state.slicing
    root=tmp_path/'source';destination=tmp_path/'cache'
    mapping={'magnet/best.onnx':b'magnet','bed/model.pt':b'bed','bed/geometry.npz':b'geometry',
             'magnet/infer.py':b'predict','bed/training/supervised.py':b'prepare'}
    for name,content in mapping.items():
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(content)
    manifest=tmp_path/'manifest.json';manifest.write_text(json.dumps({'files':[
        {'path':name,'sha256':hashlib.sha256(content).hexdigest()} for name,content in mapping.items()]}))
    m.store.settings.update(enabled=False,magnetDeployDirectory=str(root/'magnet'),magnetModelPath=str(root/'magnet/best.onnx'),
        bedDeployDirectory=str(root/'bed'),bedModelPath=str(root/'bed/model.pt'),bedGeometryPath=str(root/'bed/geometry.npz'))
    revision=m.store.settings['revision'];m.store.save_settings()
    monkeypatch.setattr('scripts.cache_runtime_models.subprocess.run',lambda *a,**k:shutil.copytree(root,destination))
    try:
        backup=cache(tmp_path/'db/collection.sqlite3',tmp_path/'bundle.zip',destination,manifest)
        assert backup.exists()
        saved=json.loads(app.state.store.db.execute('SELECT payload FROM simple_slicing_settings').fetchone()[0])
        assert saved['revision']==revision and saved['bedModelPath']==str(destination/'bed/model.pt')
        assert saved['modelCache']['sourceSettings']['bedModelPath']==str(root/'bed/model.pt')
        (root/'bed/model.pt').write_bytes(b'changed-model')
        with pytest.raises(ValueError,match='版本不同'):
            cache(tmp_path/'db/collection.sqlite3',tmp_path/'bundle.zip',tmp_path/'other',manifest)
    finally:await close_app(app)
