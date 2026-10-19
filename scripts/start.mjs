import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
process.chdir(root);
const python = path.join(root, '.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
if (!existsSync(python)) {
    console.error('请先安装 Python 后端依赖。Mac 请双击“启动简易版.command”，或按 README.md 完成安装。');
    process.exit(1);
}
const appUrl = `http://127.0.0.1:${process.env.PORT || '8786'}`;
try {
    const result = await fetch(`${appUrl}/api/health`, { signal: AbortSignal.timeout(1200) });
    if (result.ok && (await result.json()).app === 'inspection-video-gather-simple') {
        console.log(`程序已在运行：${appUrl}`);
        openBrowser();
        process.exit(0);
    }
    console.error('端口已被其他程序占用，请使用 PORT=8787 npm start。');
    process.exit(1);
} catch { /* 没有运行中的服务，继续启动。 */ }

const newest = directory => Math.max(0, ...readdirSync(directory, { withFileTypes: true }).map(entry => {
    const filename = path.join(directory, entry.name);
    return entry.isDirectory() ? newest(filename) : statSync(filename).mtimeMs;
}));
const build = path.join(root, 'dist/index.html');
if (!existsSync(build) || newest(path.join(root, 'src')) > statSync(build).mtimeMs || statSync('package-lock.json').mtimeMs > statSync(build).mtimeMs) {
    const result = spawnSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', 'build'], { stdio: 'inherit', shell: process.platform === 'win32' });
    if (result.status) process.exit(result.status);
}
const server = spawn(python, ['-m', 'app.main'], { stdio: 'inherit', cwd: root });
server.on('exit', code => process.exit(code || 0));
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => server.kill(signal));
for (let attempt = 0; attempt < 50; attempt++) {
    await new Promise(resolve => setTimeout(resolve, 200));
    if (server.exitCode !== null) break;
    try {
        if ((await fetch(`${appUrl}/api/health`)).ok) {
            console.log(`\n简易版质检视频采集已启动：${appUrl}\n关闭本终端或按 Ctrl+C 可停止本地程序。`);
            openBrowser();
            break;
        }
    } catch { /* 等待服务启动。 */ }
}

function openBrowser() {
    if (process.argv.includes('--no-open')) return;
    const command = process.platform === 'darwin' ? 'open' : process.platform === 'win32' ? 'cmd' : 'xdg-open';
    const args = process.platform === 'win32' ? ['/c', 'start', '', appUrl] : [appUrl];
    const child = spawn(command, args, { stdio: 'ignore', detached: true });
    child.on('error', () => console.log(`请在浏览器打开：${appUrl}`));
    child.unref();
}
