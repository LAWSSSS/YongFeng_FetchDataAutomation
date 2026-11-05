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
const newest = directory => Math.max(0, ...readdirSync(directory, { withFileTypes: true }).map(entry => {
    const filename = path.join(directory, entry.name);
    return entry.isDirectory() ? newest(filename) : statSync(filename).mtimeMs;
}));
const build = path.join(root, 'dist/index.html');
const buildTime = existsSync(build) ? statSync(build).mtimeMs : 0;
if (!buildTime || newest(path.join(root, 'src')) > buildTime || newest(path.join(root, 'public')) > buildTime ||
    ['index.html', 'package.json', 'package-lock.json', 'tsconfig.json', 'vite.config.ts']
        .some(file => statSync(file).mtimeMs > buildTime)) {
    console.log('正在构建网页…');
    const result = spawnSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', 'build'], { stdio: 'inherit', shell: process.platform === 'win32' });
    if (result.status !== 0) {
        console.error('网页构建失败，请查看上方第一条报错。');
        process.exit(result.status ?? 1);
    }
}
if (process.platform === 'darwin') {
    const result = spawnSync(python, [path.join(root, 'scripts/local_service.py'), 'start'], {
        encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], cwd: root,
    });
    if (result.status !== 0) process.exit(result.status ?? 1);
    const { port, instanceId, logPath } = JSON.parse(result.stdout);
    const appUrl = `http://127.0.0.1:${port}/`;
    console.log(`正在检查后台服务：${appUrl}`);
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
        try {
            const response = await fetch(`${appUrl}api/health`, { signal: AbortSignal.timeout(1200) });
            const health = response.ok ? await response.json() : null;
            if (health?.app === 'inspection-video-gather-simple' && health.instanceId === instanceId) {
                console.log(`程序已启动：${appUrl}\n关闭终端后仍会继续运行。停止：zsh ./启动简易版.command stop`);
                openBrowser(appUrl);
                process.exit(0);
            }
        } catch { /* 等待已保存连接和队列恢复。 */ }
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    console.error(`未能连接此文件夹的后台服务。请查看日志：${logPath}\n如需重新启动，请先运行 zsh ./启动简易版.command stop。`);
    process.exit(1);
}
const appUrl = `http://127.0.0.1:${process.env.PORT || '8786'}`;
const server = spawn(python, ['-m', 'app.main'], { stdio: 'inherit', cwd: root });
server.on('exit', code => process.exit(code ?? 1));
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => server.kill(signal));
for (let attempt = 0; attempt < 50; attempt++) {
    await new Promise(resolve => setTimeout(resolve, 200));
    if (server.exitCode !== null) break;
    try {
        if ((await fetch(`${appUrl}/api/health`, { signal: AbortSignal.timeout(1200) })).ok) {
            console.log(`\n简易版质检视频采集已启动：${appUrl}\n关闭本终端或按 Ctrl+C 可停止本地程序。`);
            openBrowser(appUrl);
            break;
        }
    } catch { /* 等待服务启动。 */ }
}

function openBrowser(appUrl) {
    if (process.argv.includes('--no-open')) return;
    const command = process.platform === 'darwin' ? 'open' : process.platform === 'win32' ? 'cmd' : 'xdg-open';
    const args = process.platform === 'win32' ? ['/c', 'start', '', appUrl] : [appUrl];
    const child = spawn(command, args, { stdio: 'ignore', detached: true });
    child.on('error', () => console.log(`请在浏览器打开：${appUrl}`));
    child.unref();
}
