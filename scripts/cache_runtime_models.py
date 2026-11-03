"""Move inference reads off the video disk, keeping exact model/preprocess bytes.

Pause local slicing and source uploads before running. No video or model source
is changed. The SQLite backup preserves the previous path configuration.
"""
import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KEYS = ('magnetDeployDirectory', 'magnetModelPath', 'bedDeployDirectory', 'bedModelPath', 'bedGeometryPath')


def cache(database, archive, destination, manifest_path):
    manifest = json.loads(manifest_path.read_text())
    with sqlite3.connect(database) as db:
        settings = json.loads(db.execute('SELECT payload FROM simple_slicing_settings WHERE id=1').fetchone()[0])
        upload_row = db.execute('SELECT payload FROM simple_upload_settings WHERE id=1').fetchone()
        uploads = json.loads(upload_row[0]) if upload_row else {}
        if settings.get('enabled') or uploads.get('enabled'):
            raise ValueError('先暂停本地识别和原片上传，再缓存模型。')
        originals = (settings.get('modelCache') or {}).get('sourceSettings') or {key: settings[key] for key in KEYS}
        sources = {'magnet/best.onnx': originals['magnetModelPath'], 'bed/model.pt': originals['bedModelPath'],
                   'bed/geometry.npz': originals['bedGeometryPath']}
        for entry in manifest['files']:
            relative = Path(entry['path'])
            if relative.is_absolute() or '..' in relative.parts or relative.parts[0] not in {'bed', 'magnet'}:
                raise ValueError('模型资源清单路径无效。')
            deploy = originals['bedDeployDirectory' if relative.parts[0] == 'bed' else 'magnetDeployDirectory']
            path = Path(sources.get(entry['path']) or Path(deploy).joinpath(*relative.parts[1:]))
            with path.open('rb') as stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != entry['sha256']:
                    raise ValueError('模型或预处理版本不同，未改变现有配置：' + entry['path'])
        subprocess.run([sys.executable, str(ROOT/'scripts/fetch_models.py'), '--archive', str(archive),
                        '--destination', str(destination)], check=True)
        stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        backup = database.parent / 'backups' / ('pre-local-model-cache-' + stamp + '.sqlite3')
        backup.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(backup) as target:
            db.backup(target)
        destination = destination.resolve()
        settings.update(magnetDeployDirectory=str(destination/'magnet'), magnetModelPath=str(destination/'magnet/best.onnx'),
                        bedDeployDirectory=str(destination/'bed'), bedModelPath=str(destination/'bed/model.pt'),
                        bedGeometryPath=str(destination/'bed/geometry.npz'))
        settings['modelCache'] = {'directory': str(destination), 'sourceSettings': originals,
                                 'manifestSha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest()}
        # Byte-identical resources keep their rule revision and observations.
        db.execute('UPDATE simple_slicing_settings SET payload=? WHERE id=1', (json.dumps(settings, ensure_ascii=False),))
    return backup


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--database', type=Path, default=ROOT/'data/collection.sqlite3')
    parser.add_argument('--archive', type=Path, default=ROOT/'releases/v1.1.0/models-v1.zip')
    parser.add_argument('--destination', type=Path, default=ROOT/'data/model-cache/v1')
    args=parser.parse_args()
    backup = cache(args.database, args.archive, args.destination, ROOT/'assets/models/manifest.json')
    print('模型及预处理已逐文件核验，缓存就绪；原路径保存在配置中。')
    print('Backup:', backup)
