// UI 语法门禁：把 static/ui/index.html 里的内联 <script> 抽出来交给 node 解析。
// 为什么需要：整页只有一个内联脚本块，里面任何一处语法错都会让全部函数失效——浏览器里
// 表现是"点了没反应/表格空白"，而管理台接口照常 200，只看后端日志发现不了。
// 用法：node llmGateway/scripts/check-ui-js.js   （改完 UI、打 jar 之前跑一次）
const fs = require('fs');
const os = require('os');
const path = require('path');
const cp = require('child_process');

const htmlPath = path.resolve(__dirname, '..', 'src', 'main', 'resources', 'static', 'ui', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf8');
const blocks = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
if (!blocks.length) { console.error('[FAIL] no inline <script> block found in ' + htmlPath); process.exit(1); }

let bad = 0;
blocks.forEach((code, i) => {
  const tmp = path.join(os.tmpdir(), 'gw-ui-check-' + process.pid + '-' + i + '.js');
  fs.writeFileSync(tmp, code, 'utf8');
  try {
    cp.execFileSync(process.execPath, ['--check', tmp], { stdio: ['ignore', 'pipe', 'pipe'] });
    console.log('[OK] inline script #' + (i + 1) + ' (' + code.length + ' chars) parses');
  } catch (e) {
    bad++;
    const msg = String(e.stderr || e.message || '');
    // node 报的行号在临时文件上；减掉 html 里 <script> 之前的行数才能定位回 index.html
    const htmlLine = html.slice(0, html.indexOf(code)).split('\n').length;
    console.error('[FAIL] inline script #' + (i + 1) + ' does not parse:\n'
      + msg.split('\n').map(l => l.replace(/gw-ui-check-\d+-\d+\.js/g, 'index.html(内联脚本起点约第 ' + htmlLine + ' 行)')).join('\n'));
  } finally {
    fs.rmSync(tmp, { force: true });
  }
});
console.log(bad ? '[FAIL] ' + bad + ' block(s) broken — 别打 jar，先修语法' : '[OK] ui script clean');
process.exit(bad ? 1 : 0);
