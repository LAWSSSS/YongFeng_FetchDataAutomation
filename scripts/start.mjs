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
if (process.argv.includes('--build-only')) process.exit(0);

const mac = process.platform === 'darwin';
const backend = mac
    ? ['scripts/local_service.py', 'start', ...process.argv.slice(2)]
    : ['-m', 'app.main'];
const server = spawn(python, backend, { stdio: 'inherit', cwd: root });
server.on('error', () => {
    console.error('无法启动后端，请按 README.md 检查 Python 环境。');
    process.exitCode = 1;
});
server.on('exit', code => process.exit(code ?? 1));
for (const signal of ['SIGHUP', 'SIGINT', 'SIGTERM']) process.on(signal, () => server.kill(signal));
if (!mac) await openWhenReady();

async function openWhenReady() {
    const appUrl = `http://127.0.0.1:${process.env.PORT || '8786'}`;
    for (let attempt = 0; attempt < 50; attempt++) {
        await new Promise(resolve => setTimeout(resolve, 200));
        if (server.exitCode !== null || !server.pid) break;
        try {
            const response = await fetch(`${appUrl}/api/health`, { redirect: 'error', signal: AbortSignal.timeout(1200) });
            const health = response.ok ? await response.json() : null;
            if (health?.app === 'inspection-video-gather-simple' && health.pid === server.pid) {
                console.log(`\n简易版质检视频采集已启动：${appUrl}\n关闭本终端或按 Ctrl+C 可停止本地程序。`);
                openBrowser(appUrl);
                break;
            }
        } catch { /* 等待服务启动。 */ }
    }
}

function openBrowser(appUrl) {
    if (process.argv.includes('--no-open')) return;
    const command = process.platform === 'darwin' ? 'open' : process.platform === 'win32' ? 'cmd' : 'xdg-open';
    const args = process.platform === 'win32' ? ['/c', 'start', '', appUrl] : [appUrl];
    const child = spawn(command, args, { stdio: 'ignore', detached: true });
    child.on('error', () => console.log(`请在浏览器打开：${appUrl}`));
    child.unref();
}
