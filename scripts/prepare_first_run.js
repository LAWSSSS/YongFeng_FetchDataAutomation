// Run with macOS's system JavaScript runtime, before loading bundled native code.
// The command pins this file and the inventory; approval is explicit and scoped
// to the native files in that inventory. This never changes Gatekeeper settings.
ObjC.import('Foundation');
ObjC.bindFunction('listxattr', ['long', ['char *', 'void *', 'unsigned long', 'int']]);
ObjC.bindFunction('removexattr', ['int', ['char *', 'char *', 'int']]);

var fm = $.NSFileManager.defaultManager;
var quarantine = 'com.apple.quarantine';

function fail(message) { throw new Error(message); }
function textFile(path) {
    var value = $.NSString.stringWithContentsOfFileEncodingError(path, $.NSUTF8StringEncoding, null);
    if (!value || value.isNil()) fail('无法读取首次启动清单，请重新解压完整包。');
    return ObjC.unwrap(value);
}
function attributes(path) {
    var value = fm.attributesOfItemAtPathError(path, null);
    if (!value || value.isNil()) fail('运行组件不可读取，请检查文件夹权限或重新解压。');
    return ObjC.deepUnwrap(value);
}
function relativePath(name) {
    if (typeof name !== 'string' || !name || /[\\\x00-\x1f\x7f]/.test(name) ||
        name.split('/').some(function (part) { return !part || part === '.' || part === '..'; })) {
        fail('首次启动清单包含无效路径。');
    }
    return name;
}
function regularPath(root, name, type, directories) {
    var parts = relativePath(name).split('/');
    var current = root;
    parts.forEach(function (part, index) {
        current += '/' + part;
        if (index < parts.length - 1 && directories && directories[current]) return;
        var actual = attributes(current).NSFileType;
        if (actual !== (index === parts.length - 1 ? type : 'NSFileTypeDirectory')) {
            fail('运行组件路径已改变：' + name + '。请重新解压。');
        }
        if (index < parts.length - 1 && directories) directories[current] = true;
    });
    return current;
}
function hasQuarantine(path) {
    // Do not read C errno across Objective-C/JavaScript bridge calls: unrelated
    // bridge work may change it. List names and distinguish empty from failure.
    for (var attempt = 0; attempt < 3; attempt++) {
        var size = Number($.listxattr(path, null, 0, 1)); // XATTR_NOFOLLOW
        if (size === 0) return false;
        if (size < 0 || size > 1048576) break;
        var data = $.NSMutableData.dataWithLength(size);
        var count = Number($.listxattr(path, data.mutableBytes, size, 1));
        if (count < 0 || count > size) continue; // Metadata changed between reads.
        data.length = count;
        var names = $.NSString.alloc.initWithDataEncoding(data, $.NSUTF8StringEncoding);
        if (!names || names.isNil()) break;
        return ObjC.unwrap(names).split('\0').indexOf(quarantine) >= 0;
    }
    fail('无法检查组件权限，请确认当前文件夹可读写。');
}
function signature(path, knownAttributes) {
    var a = knownAttributes || attributes(path);
    return JSON.stringify([a.NSFileType, a.NSFileSize, a.NSFileModificationDate,
        a.NSFileSystemFileNumber, a.NSFileSystemNumber, a.NSFilePosixPermissions]);
}
function checkHashes(root, entries) {
    var temporary = ObjC.unwrap($.NSTemporaryDirectory()) + 'gather-first-use-' + ObjC.unwrap($.NSUUID.UUID.UUIDString);
    if (!fm.createDirectoryAtPathWithIntermediateDirectoriesAttributesError(
        temporary, false, $({NSFilePosixPermissions: 448}), null)) fail('无法建立校验临时目录。');
    try {
        var checksumPath = temporary + '/checksums';
        var lines = entries.filter(function (entry) { return !Object.prototype.hasOwnProperty.call(entry, 'link'); })
            .map(function (entry) { return entry.sha256 + '  ' + entry.path; }).join('\n') + '\n';
        if (!$(lines).writeToFileAtomicallyEncodingError(checksumPath, true, $.NSUTF8StringEncoding, null)) {
            fail('无法保存临时校验清单。');
        }
        var task = $.NSTask.alloc.init;
        task.launchPath = '/usr/bin/shasum';
        task.arguments = $(['-a', '256', '-c', '--status', checksumPath]);
        task.currentDirectoryPath = root;
        task.environment = $({PATH: '/usr/bin:/bin', LC_ALL: 'C'});
        task.standardOutput = $.NSFileHandle.fileHandleWithNullDevice;
        task.standardError = $.NSFileHandle.fileHandleWithNullDevice;
        task.launch;
        task.waitUntilExit;
        if (task.terminationStatus !== 0) fail('组件校验未通过，未进行放行。请重新下载并完整解压。');
    } finally {
        fm.removeItemAtPathError(temporary, null);
    }
}
function verifyInventory(root, entries) {
    var expected = Object.create(null);
    var signatures = Object.create(null);
    var directories = Object.create(null);
    entries.forEach(function (entry) {
        var name = relativePath(entry.path);
        if (expected[name]) fail('首次启动清单包含重复文件。');
        expected[name] = entry;
        var isLink = Object.prototype.hasOwnProperty.call(entry, 'link');
        var path = regularPath(root, name, isLink ? 'NSFileTypeSymbolicLink' : 'NSFileTypeRegular', directories);
        if (isLink) {
            var link = ObjC.unwrap(fm.destinationOfSymbolicLinkAtPathError(path, null));
            var resolved = ObjC.unwrap($(path).stringByResolvingSymlinksInPath);
            if (typeof entry.link !== 'string' || link !== entry.link || link.charAt(0) === '/' ||
                resolved.indexOf(root + '/') !== 0 || !fm.fileExistsAtPath(resolved)) {
                fail('组件链接不属于当前程序包：' + name);
            }
        } else {
            var a = attributes(path);
            if (!/^[a-f0-9]{64}$/.test(entry.sha256) || a.NSFileSize !== entry.bytes) {
                fail('组件大小已改变：' + name);
            }
            if (a.NSFileReferenceCount !== 1) fail('组件与其他位置共用文件，请重新解压：' + name);
            signatures[name] = signature(path, a);
        }
    });
    // Finder can add .DS_Store metadata. Other unlisted files must not obtain
    // trust indirectly by being imported by a newly approved interpreter.
    var enumerator = fm.enumeratorAtPath(root);
    var next;
    while ((next = enumerator.nextObject) && !next.isNil()) {
        var name = ObjC.unwrap(next);
        var type = attributes(root + '/' + name).NSFileType;
        if (type === 'NSFileTypeDirectory') continue;
        if (name === 'first-run.json') continue;
        if (name.split('/').pop() === '.DS_Store' && type === 'NSFileTypeRegular') continue;
        if (!expected[name]) fail('运行组件内有清单之外的文件：' + name + '。请使用新解压的完整包。');
    }
    checkHashes(root, entries);
    return signatures;
}

function run(args) {
    if (args.length !== 2 || ['check', 'approve'].indexOf(args[0]) < 0) fail('首次启动参数无效。');
    var root = ObjC.unwrap($(args[1]).stringByStandardizingPath);
    if (root.charAt(0) !== '/' || attributes(root).NSFileType !== 'NSFileTypeDirectory' ||
        ObjC.unwrap($(root).stringByResolvingSymlinksInPath) !== root) fail('运行组件目录必须位于当前程序内。');
    regularPath(root, 'first-run.json', 'NSFileTypeRegular');
    var manifest = JSON.parse(textFile(root + '/first-run.json'));
    if (manifest.schema !== 1 || !Array.isArray(manifest.files) || !Array.isArray(manifest.nativeFiles) ||
        !manifest.nativeFiles.length) fail('首次启动清单不完整。');
    var files = Object.create(null);
    manifest.files.forEach(function (entry) { files[relativePath(entry.path)] = entry; });
    var pending = [];
    manifest.nativeFiles.forEach(function (name) {
        if (!files[name] || Object.prototype.hasOwnProperty.call(files[name], 'link')) fail('组件未登记。');
        var path = regularPath(root, name, 'NSFileTypeRegular');
        if (hasQuarantine(path)) pending.push(name);
    });
    if (!pending.length) return 'ready';
    if (args[0] === 'check') return 'approval-required';

    var signatures = verifyInventory(root, manifest.files);
    // No metadata is changed until every content hash and link has passed.
    pending.forEach(function (name) {
        var path = regularPath(root, name, 'NSFileTypeRegular');
        if (signature(path) !== signatures[name]) fail('校验期间组件发生变化，请重新解压后再试。');
        $.removexattr(path, quarantine, 1);
        // An interrupted prior run may have already cleared this exact file.
        // Verify the outcome rather than interpreting errno across the bridge.
        if (hasQuarantine(path)) fail('组件仍被隔离，请重新启动后再试。');
    });
    return 'ready';
}
