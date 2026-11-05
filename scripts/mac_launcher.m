#import <Cocoa/Cocoa.h>
#include <string.h>

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        NSBundle *bundle = [NSBundle mainBundle];
        BOOL stopOnly = [[bundle objectForInfoDictionaryKey:@"IVGStopOnly"] boolValue];
        BOOL stopping = stopOnly || (argc > 1 && strcmp(argv[1], "stop") == 0);
        NSURL *app = stopOnly ? [[bundle.bundleURL URLByDeletingLastPathComponent]
            URLByAppendingPathComponent:@"采集助手.app" isDirectory:YES] : bundle.bundleURL;
        NSString *resources = [app.path stringByAppendingPathComponent:@"Contents/Resources"];
        if ([bundle.bundlePath containsString:@"/AppTranslocation/"]) {
            [NSApplication sharedApplication];
            NSAlert *alert = [[NSAlert alloc] init];
            alert.messageText = @"请先把采集助手移到固定位置";
            alert.informativeText = @"在访达中创建一个可写文件夹，例如“文稿/采集助手”。将解压得到的两个应用和使用说明一起拖入新文件夹，再双击打开。已有“数据”文件夹时，请先停止旧版，再一同移动。";
            [alert runModal];
            return 1;
        }
        NSTask *task = [[NSTask alloc] init];
        task.executableURL = [NSURL fileURLWithPath:[resources stringByAppendingPathComponent:@"runtime/backend/bin/python3.13"]];
        task.arguments = @[@"-I", @"-B", [resources stringByAppendingPathComponent:@"application/launcher.py"], stopping ? @"stop" : @"launch"];
        NSMutableDictionary *environment = [[[NSProcessInfo processInfo] environment] mutableCopy];
        for (NSString *key in [environment.allKeys copy]) {
            if ([key hasPrefix:@"PYTHON"] || [key hasPrefix:@"DYLD_"] || [key hasPrefix:@"LD_"]) [environment removeObjectForKey:key];
        }
        task.environment = environment;
        NSPipe *errors = [NSPipe pipe];
        task.standardError = errors;
        task.standardOutput = [NSFileHandle fileHandleWithNullDevice];
        NSError *error = nil;
        BOOL started = [task launchAndReturnError:&error];
        NSData *diagnostic = started ? [[errors fileHandleForReading] readDataToEndOfFile] : nil;
        if (started) [task waitUntilExit];
        if (!started || task.terminationStatus != 0) {
            NSString *message = started ? [[NSString alloc] initWithData:diagnostic encoding:NSUTF8StringEncoding] :
                (stopOnly ? @"请把“停止采集助手”和“采集助手”放在同一文件夹后重试。" : @"无法打开内置运行环境，请重新解压完整文件夹。");
            [NSApplication sharedApplication];
            NSAlert *alert = [[NSAlert alloc] init];
            alert.messageText = stopping ? @"采集助手暂未停止" : @"采集助手暂未启动";
            alert.informativeText = message.length ? message : @"请检查独立数据目录中的启动日志。";
            [alert addButtonWithTitle:@"知道了"];
            [NSApp activateIgnoringOtherApps:YES];
            [alert runModal];
            return 1;
        }
    }
    return 0;
}
