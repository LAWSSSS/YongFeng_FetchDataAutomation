#import <Cocoa/Cocoa.h>

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        NSBundle *bundle = [NSBundle mainBundle];
        NSString *resources = [bundle resourcePath];
        NSTask *task = [[NSTask alloc] init];
        task.executableURL = [NSURL fileURLWithPath:[resources stringByAppendingPathComponent:@"runtime/backend/bin/python3.13"]];
        task.arguments = @[@"-I", @"-B", [resources stringByAppendingPathComponent:@"application/launcher.py"], @"launch"];
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
        if (started) [task waitUntilExit];
        if (!started || task.terminationStatus != 0) {
            NSString *message = started ? [[NSString alloc] initWithData:[[errors fileHandleForReading] readDataToEndOfFile] encoding:NSUTF8StringEncoding] : @"无法打开内置运行环境，请重新解压完整文件夹。";
            [NSApplication sharedApplication];
            NSAlert *alert = [[NSAlert alloc] init];
            alert.messageText = @"采集助手暂未启动";
            alert.informativeText = message.length ? message : @"请检查独立数据目录中的启动日志。";
            [alert addButtonWithTitle:@"知道了"];
            [alert runModal];
            return 1;
        }
    }
    return 0;
}
