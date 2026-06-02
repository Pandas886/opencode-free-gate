#!/usr/bin/env bun

/**
 * opencode-free-gate — 自动代理池 + opencode.ai/zen 反代网关
 *
 * 从 proxy.amux.ai 获取免费代理，自动故障切换，解除 opencode 免费模型的额度限制。
 * 同时兼容 OpenAI 和 Anthropic 格式。
 *
 * 使用:
 *   bun run gate.ts
 *   PORT=8080 bun run gate.ts
 */

import https from 'node:https';
import { HttpsProxyAgent } from 'hpagent';
import { SocksProxyAgent } from 'socks-proxy-agent';

interface ProxyItem {
  address: string;
  protocol: string;
  latency: number;
  quality_grade: string;
}

const PROXY_API = 'https://proxy.amux.ai/api/proxies';
const UPSTREAM = 'https://opencode.ai/zen';
const PORT = parseInt(process.env.PORT || '13339');
const MAX_RETRIES = 3;
const POLL_INTERVAL = 5 * 60 * 1000;
const TIMEOUT = 120000;

let pool: ProxyItem[] = [];
let cursor = 0;
let blacklist = new Set<string>();
let current: { url: string; addr: string; proto: 'http' | 'socks5' } | null = null;

/** 转发到上游时保留的请求头 */
const FORWARD = [
  'authorization',
  'x-opencode-project',
  'x-opencode-session',
  'x-opencode-request',
  'x-opencode-client',
  'content-type',
  'accept',
  'anthropic-version',
  'anthropic-beta',
];

// ––––––––––––––––––––––––––––––––––––––––––––––––––––
//  代理池
// ––––––––––––––––––––––––––––––––––––––––––––––––––––

async function loadPool(): Promise<void> {
  const res = await fetch(PROXY_API);
  const all: any[] = await res.json();
  pool = all
    .filter((p) => p.quality_grade === 'S' && p.status === 'active')
    .sort((a, b) => a.latency - b.latency);
  cursor = 0;
  blacklist.clear();
  current = null;
  console.log(`[池] ${pool.length} 个 S 级代理`);
}

/** 无当前代理时选一个；已有则直接返回 true */
function selectProxy(): boolean {
  if (current) return true;
  for (let i = 0; i < pool.length; i++) {
    const j = cursor % pool.length;
    cursor = j + 1;
    const p = pool[j];
    if (!blacklist.has(p.address)) {
      const url = p.protocol === 'socks5'
        ? `socks5h://${p.address}`
        : `http://${p.address}`;
      current = { url, addr: p.address, proto: p.protocol as 'http' | 'socks5' };
      console.log(`[选] ${p.address} (${p.protocol})`);
      return true;
    }
  }
  return false;
}

/** 标记代理不可用 */
function drop(addr: string) {
  blacklist.add(addr);
  console.log(`[弃] ${addr} (${blacklist.size}/${pool.length})`);
}

// ––––––––––––––––––––––––––––––––––––––––––––––––––––
//  请求处理
// ––––––––––––––––––––––––––––––––––––––––––––––––––––

function collectHeaders(req: Request): Record<string, string> {
  const h: Record<string, string> = {};
  for (const k of FORWARD) {
    const v = req.headers.get(k);
    if (v) h[k] = v;
  }
  if (!h['authorization']) h['authorization'] = 'Bearer public';
  if (!h['x-opencode-client']) h['x-opencode-client'] = 'cli';
  if (!h['content-type']) h['content-type'] = 'application/json';
  return h;
}

function makeAgent(proxyUrl: string, proto: 'http' | 'socks5'): https.Agent {
  if (proto === 'socks5') {
    return new SocksProxyAgent(proxyUrl, { timeout: 10000 }) as unknown as https.Agent;
  }
  return new HttpsProxyAgent({
    proxy: proxyUrl,
    keepAlive: false,
    timeout: 10000,
  }) as unknown as https.Agent;
}

/** 非流式请求 */
function doHttps(
  path: string,
  method: string,
  headers: Record<string, string>,
  body: string | undefined,
  agent: https.Agent,
): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = https.request(
      `${UPSTREAM}${path}`,
      { method, headers, agent, timeout: TIMEOUT, rejectUnauthorized: false },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode || 200, body: Buffer.concat(chunks).toString('utf-8') }));
        res.on('error', reject);
      },
    );
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('超时')));
    if (body) req.write(body);
    req.end();
  });
}

/** 流式请求 — 返回 ReadableStream 给 Bun Response */
function doHttpsStream(
  path: string,
  method: string,
  headers: Record<string, string>,
  body: string | undefined,
  agent: https.Agent,
): Promise<{ status: number; stream: ReadableStream<Uint8Array> }> {
  return new Promise((resolve, reject) => {
    const req = https.request(
      `${UPSTREAM}${path}`,
      { method, headers, agent, timeout: 300000, rejectUnauthorized: false },
      (res) => {
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            res.on('data', (chunk: Buffer) => controller.enqueue(new Uint8Array(chunk)));
            res.on('end', () => controller.close());
            res.on('error', (err) => controller.error(err));
          },
          cancel() { res.destroy(); },
        });
        resolve({ status: res.statusCode || 200, stream });
      },
    );
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

/** 核心：通过当前代理转发请求，失败时自动切代理重试 */
async function proxy(
  path: string,
  method: string,
  headers: Record<string, string>,
  body?: string,
  retry = 0,
): Promise<Response> {
  if (!pool.length) await loadPool();

  if (!selectProxy()) {
    await loadPool();
    if (!selectProxy()) {
      return new Response('{"error":"没有可用代理"}', { status: 502, headers: { 'content-type': 'application/json' } });
    }
  }

  const { url, addr, proto } = current!;
  const isStream = (headers['accept'] || '').includes('event-stream');
  const agent = makeAgent(url, proto);

  try {
    if (isStream) {
      const { stream } = await doHttpsStream(path, method, headers, body, agent);
      return new Response(stream, {
        status: 200,
        headers: { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache' },
      });
    }

    const { status, body: respBody } = await doHttps(path, method, headers, body, agent);
    agent.destroy();

    if (status >= 500) throw new Error(`上游 ${status}`);

    return new Response(respBody, { status, headers: { 'content-type': 'application/json; charset=utf-8' } });
  } catch (e: any) {
    console.error(`[错] ${addr}: ${e.message}`);
    agent.destroy();
    drop(addr);
    current = null;
    if (retry < MAX_RETRIES) return proxy(path, method, headers, body, retry + 1);
    return new Response(JSON.stringify({ error: `所有代理失败: ${e.message}` }), {
      status: 502,
      headers: { 'content-type': 'application/json' },
    });
  }
}

// ––––––––––––––––––––––––––––––––––––––––––––––––––––
//  路由
// ––––––––––––––––––––––––––––––––––––––––––––––––––––

/** 路径归一化: /openai/v1/* 或 /anthropic/v1/* → /v1/* */
function normalize(raw: string): string | null {
  const m = raw.match(/^\/(openai|anthropic)(\/v1\/.+)$/);
  if (m) return m[2];
  return null;
}

// ––––––––––––––––––––––––––––––––––––––––––––––––––––
//  服务
// ––––––––––––––––––––––––––––––––––––––––––––––––––––

console.log(`[门] http://localhost:${PORT}`);
console.log(`[门] OpenAI:    /openai/v1/chat/completions | /openai/v1/models`);
console.log(`[门] Anthropic: /anthropic/v1/messages`);

Bun.serve({
  port: PORT,
  idleTimeout: 0,
  async fetch(req) {
    const { pathname: raw, search } = new URL(req.url);
    const method = req.method;
    const pathname = normalize(raw);
    console.log(`[>] ${method} ${raw}`);

    if (!pathname) {
      if (raw === '/' || raw === '/v1') {
        return new Response(JSON.stringify({ status: 'ok', proxies: pool.length }), {
          headers: { 'content-type': 'application/json' },
        });
      }
      return new Response('{"error":"not found"}', { status: 404, headers: { 'content-type': 'application/json' } });
    }

    if (pathname === '/v1/models' && method === 'GET') {
      return proxy(pathname + search, 'GET', collectHeaders(req));
    }

    if ((pathname === '/v1/chat/completions' || pathname === '/v1/messages') && method === 'POST') {
      let body = await req.text();
      const h = collectHeaders(req);
      const isStream =
        h['accept']?.includes('event-stream') ||
        (() => { try { return JSON.parse(body).stream; } catch { return false; } })();
      if (isStream) {
        h['accept'] = 'text/event-stream';
        try {
          const json = JSON.parse(body);
          if (!json.stream) { json.stream = true; body = JSON.stringify(json); }
        } catch {}
      }
      return proxy(pathname, 'POST', h, body);
    }

    return new Response('{"error":"not found"}', { status: 404, headers: { 'content-type': 'application/json' } });
  },
});

loadPool().catch((e) => console.error('[池] 加载失败:', e));
setInterval(() => loadPool().catch(() => {}), POLL_INTERVAL);
