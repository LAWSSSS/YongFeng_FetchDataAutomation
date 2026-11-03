"""Evidence required before a video's photos or original are archived or deleted."""
from .workspace_files import compatible
from .package_import_reuse import image_identity


TERMINAL_EMPTY_ERRORS = {'no_decodable_samples', 'no_cuttable_fragments'}


def verified_empty_conclusion(item, name):
    """A zero-photo conclusion is evidence, not a failed-worker label."""
    category = (item.get('categories') or {}).get(name) or {}
    proof = category.get('processingConclusion') or {}
    outcome = proof.get('outcome')
    requested, decoded, skipped = (proof.get(key) for key in
                                    ('requestedSamples', 'decodedSamples', 'skippedSamples'))
    expected, actual = proof.get('fileVersion') or {}, item.get('fileVersion') or {}
    if (proof.get('version') != 1 or proof.get('completed') is not True
            or outcome not in {'no_target', 'no_usable_frames'}
            or not all(type(value) is int and value >= 0 for value in (requested, decoded, skipped))
            or requested <= 0 or requested != decoded + skipped
            or not {'device', 'inode', 'size', 'modifiedNs'}.issubset(expected)
            or not compatible(actual, expected)):
        return False
    return ((outcome == 'no_target' and category.get('state') == 'no_target' and decoded > 0)
            or (outcome == 'no_usable_frames' and category.get('state') == 'uncuttable'
                and decoded == 0 and skipped == requested))


def complete_image_ids(ids, count):
    if not isinstance(ids, list) or len(ids) != count:
        return False
    identities = [image_identity(value) for value in ids]
    return None not in identities and len(set(identities)) == count


def verified_package(package, item, name):
    if (not package or package.get('superseded') or package.get('state') != 'complete'
            or package.get('itemId') != item.get('id') or package.get('category') != name
            or not package.get('verifiedJobId') or not package.get('jobId')
            or str(package['verifiedJobId']) != str(package['jobId'])):
        return False
    count = package.get('imageCount')
    if (type(count) is not int or count <= 0 or type(package.get('actualImages')) is not int
            or package['actualImages'] != count):
        return False
    ids = package.get('outputImageIds') or []
    if not complete_image_ids(ids, count):
        return False
    expected, actual = package.get('videoFileVersion') or {}, item.get('fileVersion') or {}
    if not {'device', 'inode', 'size', 'modifiedNs'}.issubset(expected) or not compatible(actual, expected):
        return False
    category = (item.get('categories') or {}).get(name) or {}
    if category.get('targetDatasetId') and str(category['targetDatasetId']) != str(package.get('dataset') or ''):
        return False
    if category.get('actualImages') is not None and category.get('actualImages') != count:
        return False
    return True


def _shared_package_verified(package, item, name, items):
    """Accept an explicit shared receipt only through its verified live owner.

    Local registration and an older platform binding can describe the same
    processing version. Their category callbacks reference one package; they
    are not separate imports. A filename or a complete marker is never proof.
    """
    owner = next((row for row in items if row.get('id') == package.get('itemId')), None)
    category = (item.get('categories') or {}).get(name) or {}
    if (not owner or owner.get('superseded') or not item.get('mediaId')
            or item.get('mediaId') != owner.get('mediaId')
            or type(item.get('mediaGeneration')) is not int
            or type(owner.get('mediaGeneration')) is not int
            or item['mediaGeneration'] != owner['mediaGeneration']
            or item.get('collectionVideoId') != owner.get('collectionVideoId')
            or item.get('collectionRevision') != owner.get('collectionRevision')
            or not item.get('path') or item.get('path') != owner.get('path')
            or not compatible(item.get('fileVersion') or {}, owner.get('fileVersion') or {})
            or not verified_package(package, owner, name)
            or ((owner.get('categories') or {}).get(name) or {}).get('state') != 'complete'
            or ((owner.get('categories') or {}).get(name) or {}).get('packageId') != package.get('id')
            or category.get('taskId') or category.get('operationId')
            or (category.get('importJobId') and str(category['importJobId']) != str(package.get('jobId')))
            or str(category.get('targetDatasetId') or '') != str(package.get('dataset') or '')
            or type(category.get('actualImages')) is not int
            or category['actualImages'] != package.get('actualImages')):
        return False
    expected, actual = package.get('videoFileVersion') or {}, item.get('fileVersion') or {}
    return {'device', 'inode', 'size', 'modifiedNs'}.issubset(actual) and compatible(actual, expected)


def category_verified(item, name, packages, items=()):
    category = (item.get('categories') or {}).get(name) or {}
    state = category.get('state')
    related = [p for p in packages if p.get('itemId') == item.get('id') and p.get('category') == name
               and not p.get('superseded')]
    empty = state == 'no_target' or verified_empty_conclusion(item, name) or (item.get('partialRecovery') and state == 'attention'
             and (category.get('errorCode') or item.get('errorCode')) in TERMINAL_EMPTY_ERRORS)
    if empty:
        return not related
    if state != 'complete':
        return False
    if related:
        package_id = category.get('packageId')
        return (len(related) == 1 and (not package_id or related[0].get('id') == package_id)
                and verified_package(related[0], item, name))
    if category.get('packageId'):
        # Reused results must identify one package whose original owner is
        # present in this exact video/version projection and verified itself.
        referenced = [p for p in packages if p.get('id') == category['packageId']]
        return len(referenced) == 1 and _shared_package_verified(referenced[0], item, name, items)
    # Legacy platform slicing can directly produce verified target images.
    count = category.get('actualImages')
    ids = category.get('outputImageIds') or []
    return (bool(category.get('taskId')) and bool(category.get('targetDatasetId'))
            and type(count) is int and count > 0 and complete_image_ids(ids, count))


def photo_results_verified(items, packages):
    """Require both categories, unique bound results, and every generated package."""
    items = [item for item in items if item and not item.get('superseded')]
    if not items:
        return False
    by_name = {'bed': [], 'magnet': []}
    for item in items:
        for name in by_name:
            state = (item.get('categories') or {}).get(name, {}).get('state')
            if state and state != 'not_applicable':
                by_name[name].append(item)
    if any(not rows or not all(category_verified(item, name, packages, items) for item in rows)
           for name, rows in by_name.items()):
        return False
    current_ids = {item['id'] for item in items}
    for package in packages:
        if package.get('itemId') not in current_ids or package.get('superseded'):
            continue
        item = next(item for item in items if item['id'] == package['itemId'])
        if not verified_package(package, item, package.get('category')):
            return False
    return True
