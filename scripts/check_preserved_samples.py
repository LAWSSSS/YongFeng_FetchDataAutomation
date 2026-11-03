"""Read-only real sample checks. Inference output is never invented for coverage."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.frame_decode import decode_position
from app.slicing import source_version, ffprobe_file
from scripts.reset_managed_data import sha
from scripts.verify_mac import PARITY_SCRIPT


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    settings = json.loads(args.settings.read_text())
    report = []
    for kind, pattern, positions in [('normal', '正常/*.mp4', [600]),
                                     ('abnormal', '异常/*.mp4', [270, 660, 5760])]:
        paths = list(args.samples.glob(pattern))
        if len(paths) != 1:
            raise ValueError('Each sample must be bound uniquely.')
        path = paths[0].resolve()
        version = source_version(path)
        checksum = sha(path)
        metadata = ffprobe_file(path, settings['frameToolPath'])
        records, images = [], []
        for point in positions:
            frame = decode_position(path, settings['frameToolPath'], point, metadata, expected=version)
            record = frame.observation()
            record['decoded'] = frame.image is not None
            if frame.image:
                image = args.output / f'{kind}-{point}.jpg'
                image.write_bytes(frame.image)
                images.append(image.resolve())
            records.append(record)
        request = {'application': str(ROOT), 'settings': settings, 'samples': list(map(str, images))}
        result = subprocess.run([settings['workerPythonPath'], '-I', '-B', '-c', PARITY_SCRIPT],
            input=json.dumps(request), text=True, capture_output=True, timeout=180)
        if result.returncode:
            raise RuntimeError('Real model sample inference failed.')
        labels = json.loads(result.stdout)
        with zipfile.ZipFile(args.output / f'{kind}-actual-frames.zip', 'w', zipfile.ZIP_STORED) as archive:
            for image in images:
                archive.write(image, image.name)
        with zipfile.ZipFile(args.output / f'{kind}-actual-frames.zip') as archive:
            assert len(archive.namelist()) == len(images) and not archive.testzip()
        assert source_version(path) == version and sha(path) == checksum
        if kind == 'abnormal':
            assert records[-1]['decoded'] is False and all(r['decoded'] for r in records[:-1])
        else:
            assert all(r['decoded'] for r in records)
        report.append({'sample': kind, 'sha256': checksum, 'metadata': metadata,
                       'positions': records, 'realModelOutputs': labels, 'exportedImages': len(images),
                       'sourceUnchanged': True})
    (args.output / 'report.json').write_text(json.dumps({'passed': True, 'checks': report}, ensure_ascii=False, indent=2))
    print(json.dumps({'passed': True, 'samples': len(report), 'images': sum(r['exportedImages'] for r in report)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', type=Path, required=True)
    parser.add_argument('--settings', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    run(parser.parse_args())
