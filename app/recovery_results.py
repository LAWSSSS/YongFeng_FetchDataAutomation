"""Completion receipts for partial media; never authority to replace an original."""


def verified_result(item, get_package):
    from .photo_completion import verified_package
    if (not item or not item.get('collectionRecovery') or item.get('superseded')
            or item.get('state') not in {'complete', 'no_target', 'uncuttable'}):
        return None
    result = {}
    for name in ('bed', 'magnet'):
        category = item.get('categories', {}).get(name) or {}
        if category.get('state') == 'uncuttable':
            from .photo_completion import verified_empty_conclusion
            if not verified_empty_conclusion(item, name):
                return None
            result[name] = {'state': 'uncuttable', 'images': 0, 'endReason': 'no_usable_frames'}
            continue
        if category.get('state') == 'no_target':
            result[name] = {'state': 'no_target', 'images': 0}
            continue
        package = get_package(category.get('packageId')) if category.get('packageId') else None
        if category.get('state') != 'complete' or not verified_package(package, item, name):
            return None
        result[name] = {'state': 'complete', 'packageId': package['id'],
                        'jobId': str(package['verifiedJobId']), 'images': package['imageCount']}
    return result


def partial_complete(version):
    recovery = version.get('recovery') or {}
    return recovery.get('state') == 'partial_complete' and recovery.get('resultVerified') is True
