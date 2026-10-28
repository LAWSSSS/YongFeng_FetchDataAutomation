"""Completion receipts for partial media; never authority to replace an original."""


def verified_result(item, get_package):
    from .workspace_files import compatible
    if (not item or not item.get('collectionRecovery') or item.get('superseded')
            or item.get('state') not in {'complete', 'no_target'}):
        return None
    result = {}
    for name in ('bed', 'magnet'):
        category = item.get('categories', {}).get(name) or {}
        if category.get('state') == 'no_target':
            result[name] = {'state': 'no_target', 'images': 0}
            continue
        package = get_package(category.get('packageId')) if category.get('packageId') else None
        if (category.get('state') != 'complete' or not package or package.get('superseded')
                or package.get('itemId') != item['id'] or package.get('category') != name
                or package.get('state') != 'complete' or not package.get('verifiedJobId')
                or not package.get('imageCount') or package.get('actualImages') != package['imageCount']
                or len(set(map(str, package.get('outputImageIds') or []))) != package['imageCount']
                or not package.get('videoFileVersion') or not item.get('fileVersion')
                or 'size' not in package['videoFileVersion']
                or not compatible(item['fileVersion'], package['videoFileVersion'])):
            return None
        result[name] = {'state': 'complete', 'packageId': package['id'],
                        'jobId': str(package['verifiedJobId']), 'images': package['imageCount']}
    return result


def partial_complete(version):
    recovery = version.get('recovery') or {}
    return recovery.get('state') == 'partial_complete' and recovery.get('resultVerified') is True
