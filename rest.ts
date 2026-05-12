/**
 * REST channel adapter for NanoClaw.
 *
 */
import { randomBytes } from 'node:crypto'
import http from 'node:http'

import { readEnvFile } from '../env.js'
import { log } from '../log.js'
import type { ChannelAdapter, ChannelAdapterFactory, ChannelSetup, InboundMessage, OutboundMessage } from './adapter.js'

const REST_ENV = {
  PORT: 'REST_PORT',
  MESSAGES_URL: 'REST_MESSAGES_URL',
  LIFECYCLE_READY_URL: 'REST_LIFECYCLE_READY_URL',
  OUTBOUND_TOKEN: 'REST_OUTBOUND_TOKEN',
  TRUSTED_HEADER_NAME: 'REST_TRUSTED_HEADER_NAME',
  TRUSTED_HEADER_VALUE: 'REST_TRUSTED_HEADER_VALUE',
  DEV_BEARER: 'REST_DEV_BEARER',
  DISPLAY_NAME: 'NANOCLAW_DISPLAY_NAME',
} as const
import { registerChannelAdapter } from './channel-registry.js'

const ADAPTER_VERSION = '0.1.0' // POC; nanopod has no per-channel package.json

export interface RestAdapterOptions {
  port: number
  messagesUrl: string
  lifecycleReadyUrl: string | null
  outboundToken: string
  trustedHeaderName: string
  trustedHeaderValue: string
  devBearer: string | null
  displayName?: string
}

// Inbound body validation — manual rather than Zod because nanoclaw (the
// deploy target) doesn't depend on Zod. Keeping source and deploy copies
// byte-identical so the sync script is a pure file copy with no rewrites.
interface InboundBody {
  messageId: string
  content: string
  replyContext?: Record<string, unknown>
}

type ValidationResult = { ok: true; data: InboundBody } | { ok: false; error: string }

function validateInboundBody(parsed: unknown): ValidationResult {
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { ok: false, error: 'body must be a JSON object' }
  }
  const obj = parsed as Record<string, unknown>
  if (typeof obj.messageId !== 'string' || obj.messageId.length === 0) {
    return { ok: false, error: 'messageId: must be a non-empty string' }
  }
  if (typeof obj.content !== 'string' || obj.content.length === 0) {
    return { ok: false, error: 'content: must be a non-empty string' }
  }
  let replyContext: Record<string, unknown> | undefined
  if (obj.replyContext !== undefined) {
    if (typeof obj.replyContext !== 'object' || obj.replyContext === null || Array.isArray(obj.replyContext)) {
      return { ok: false, error: 'replyContext: must be an object' }
    }
    replyContext = obj.replyContext as Record<string, unknown>
  }
  return { ok: true, data: { messageId: obj.messageId, content: obj.content, replyContext } }
}

async function readBody(req: http.IncomingMessage): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(chunk as Buffer)
  return Buffer.concat(chunks).toString('utf8')
}

function sendJson(res: http.ServerResponse, status: number, payload: unknown): void {
  res.statusCode = status
  res.setHeader('Content-Type', 'application/json')
  res.end(JSON.stringify(payload))
}

function extractText(content: unknown): string | undefined {
  if (typeof content === 'string') return content
  if (content !== null && typeof content === 'object' && 'text' in content) {
    const text = (content as { text: unknown }).text
    if (typeof text === 'string') return text
  }
  return undefined
}

async function postJson(url: string, body: string, token: string): Promise<number> {
  const u = new URL(url)
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: u.hostname,
        port: u.port || (u.protocol === 'https:' ? 443 : 80),
        path: u.pathname + u.search,
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      },
      (res) => {
        // Drain body so the socket can be reused / closed cleanly.
        res.on('data', () => {})
        res.on('end', () => resolve(res.statusCode ?? 0))
      },
    )
    req.on('error', reject)
    req.write(body)
    req.end()
  })
}

export function createRestAdapter(opts: RestAdapterOptions): ChannelAdapter {
  let server: http.Server | undefined
  let setupConfig: ChannelSetup | undefined
  // Most-recent inbound's replyContext, echoed on outbound. Captured by the
  // inbound handler and read by `deliver` below.
  let lastReplyContext: object | undefined

  const setup = async (config: ChannelSetup): Promise<void> => {
    setupConfig = config
    server = http.createServer((req, res) => {
      // Wrap the async handler so unexpected throws don't tear down the server.
      void (async () => {
        try {
          const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`)
          if (url.pathname !== '/message') {
            res.statusCode = 404
            res.end()
            return
          }
          if (req.method !== 'POST') {
            res.statusCode = 405
            res.end()
            return
          }
          // Auth: trusted header is authoritative when present.
          // Dev bearer fallback is only consulted when the trusted header is absent.
          const trustedHeaderKey = opts.trustedHeaderName.toLowerCase()
          const presentedValue = req.headers[trustedHeaderKey]
          if (typeof presentedValue === 'string') {
            if (presentedValue !== opts.trustedHeaderValue) {
              log.warn('rest inbound: trusted header mismatch — 401', { header: opts.trustedHeaderName })
              sendJson(res, 401, { error: 'Unauthorized' })
              return
            }
            log.info('rest inbound: trusted header matched')
            // matched — fall through to handle the body
          } else {
            // trusted header absent: consult dev bearer fallback.
            if (opts.devBearer != null) {
              const auth = req.headers['authorization']
              if (typeof auth === 'string' && auth === `Bearer ${opts.devBearer}`) {
                log.info('rest inbound: accepted via dev bearer')
                // accepted via dev bearer — fall through
              } else {
                log.warn('rest inbound: missing trusted header and invalid/absent dev bearer — 401')
                sendJson(res, 401, { error: 'Unauthorized' })
                return
              }
            } else {
              log.warn('rest inbound: missing trusted header, no dev bearer configured — 401')
              sendJson(res, 401, { error: 'Unauthorized' })
              return
            }
          }

          // Read and parse body.
          const raw = await readBody(req)
          let parsed: unknown
          try {
            parsed = JSON.parse(raw)
          } catch {
            sendJson(res, 400, { error: 'Invalid JSON' })
            return
          }
          const result = validateInboundBody(parsed)
          if (!result.ok) {
            sendJson(res, 400, { error: result.error })
            return
          }
          const body = result.data

          // Capture replyContext for echo on subsequent outbound (T9 round-trip).
          lastReplyContext = body.replyContext

          const inbound: InboundMessage = {
            id: body.messageId,
            kind: 'chat',
            // `senderId: 'nanopod'` (no colon) — nanoclaw's permissions module
            // resolves this as `${channelType}:${rawHandle}` = `rest:nanopod`,
            // matching the user registered at provisioning time. The relay is the
            // wire conduit, not the user; for the single-conversation invariant
            // the user identity collapses to the bot identity.
            content: { text: body.content, sender: 'nanopod', senderId: 'nanopod' },
            timestamp: new Date().toISOString(),
          }

          log.info('rest inbound: routing message', { messageId: body.messageId, contentLen: body.content.length })
          // Router-side throws should not tear down the HTTP server. The 202 is
          // emitted regardless — the message was accepted by the channel even
          // if downstream blew up. Mirrors emacs.ts posture.
          try {
            await setupConfig?.onInbound('rest:nanopod', null, inbound)
            log.info('rest inbound: onInbound returned ok', { messageId: body.messageId })
          } catch (err) {
            log.error('rest inbound: onInbound threw', { messageId: body.messageId, err: String(err) })
          }

          sendJson(res, 202, { messageId: body.messageId })
        } catch (err) {
          log.error('rest channel request handler error', { err: String(err) })
          if (!res.headersSent) {
            try {
              sendJson(res, 500, { error: 'Internal Server Error' })
            } catch {
              // best-effort
            }
          }
        }
      })()
    })
    await new Promise<void>((resolve) => server!.listen(opts.port, '0.0.0.0', resolve))
    log.info('rest adapter listening', {
      port: opts.port,
      trustedHeader: opts.trustedHeaderName,
      devBearer: opts.devBearer != null,
    })

    config.onMetadata('rest:nanopod', opts.displayName ?? 'NanoPod', false)

    // Lifecycle ready POST — optional, best-effort. The channel is considered
    // up once the listener is bound regardless of whether the relay is notified.
    if (opts.lifecycleReadyUrl !== null) {
      const readyBody = JSON.stringify({
        adapterVersion: ADAPTER_VERSION,
        startedAt: new Date().toISOString(),
      })
      try {
        const status = await postJson(opts.lifecycleReadyUrl, readyBody, opts.outboundToken)
        if (status < 200 || status >= 300) {
          log.warn('rest channel: lifecycle/ready returned non-2xx', { url: opts.lifecycleReadyUrl, status })
        }
      } catch (err) {
        log.warn('rest channel: lifecycle/ready POST threw', { url: opts.lifecycleReadyUrl, error: String(err) })
      }
    }
  }

  const teardown = async (): Promise<void> => {
    if (!server) return
    await new Promise<void>((resolve) => server!.close(() => resolve()))
    server = undefined
    setupConfig = undefined
    lastReplyContext = undefined
  }

  const isConnected = (): boolean => server?.listening ?? false

  const deliver = async (
    platformId: string,
    _threadId: string | null,
    message: OutboundMessage,
  ): Promise<string | undefined> => {
    if (platformId !== 'rest:nanopod') {
      log.warn('rest channel: ignoring deliver for non-nanopod platformId', { platformId })
      return undefined
    }
    const text = extractText(message.content)
    if (text === undefined || text === '') return undefined

    const messageId = `rest-out-${Date.now()}-${randomBytes(4).toString('hex')}`
    const body = JSON.stringify({
      messageId,
      content: text,
      ...(lastReplyContext !== undefined ? { replyContext: lastReplyContext } : {}),
    })
    try {
      const status = await postJson(opts.messagesUrl, body, opts.outboundToken)
      if (status >= 200 && status < 300) return messageId
      log.error('rest channel: outbound POST failed', { url: opts.messagesUrl, status })
      return undefined
    } catch (err) {
      log.error('rest channel: outbound POST threw', { url: opts.messagesUrl, error: String(err) })
      return undefined
    }
  }

  return {
    name: 'rest',
    channelType: 'rest',
    supportsThreads: false,
    setup,
    teardown,
    isConnected,
    deliver,
  }
}

export const restFactory: ChannelAdapterFactory = () => {
  const env = readEnvFile([
    REST_ENV.PORT,
    REST_ENV.MESSAGES_URL,
    REST_ENV.LIFECYCLE_READY_URL,
    REST_ENV.OUTBOUND_TOKEN,
    REST_ENV.TRUSTED_HEADER_NAME,
    REST_ENV.TRUSTED_HEADER_VALUE,
    REST_ENV.DEV_BEARER,
    REST_ENV.DISPLAY_NAME,
  ])

  const required: Record<string, string | undefined> = {
    [REST_ENV.MESSAGES_URL]: env[REST_ENV.MESSAGES_URL],
    [REST_ENV.OUTBOUND_TOKEN]: env[REST_ENV.OUTBOUND_TOKEN],
    [REST_ENV.TRUSTED_HEADER_NAME]: env[REST_ENV.TRUSTED_HEADER_NAME],
    [REST_ENV.TRUSTED_HEADER_VALUE]: env[REST_ENV.TRUSTED_HEADER_VALUE],
  }

  for (const [key, value] of Object.entries(required)) {
    if (!value) {
      log.warn('rest channel disabled — required env var missing', { key })
      return null
    }
  }

  // 8000 matches the exe.dev edge convention: `https://<vmname>.exe.xyz/`
  // forwards to TCP 8000 inside the VM by default. Other ports are reachable
  // only via `ssh exe.dev share port <vmname> <port>` in the 3000–9999 range.
  const port = env[REST_ENV.PORT] ? parseInt(env[REST_ENV.PORT]!, 10) : 8000

  return createRestAdapter({
    port,
    messagesUrl: env[REST_ENV.MESSAGES_URL]!,
    lifecycleReadyUrl: env[REST_ENV.LIFECYCLE_READY_URL] ?? null,
    outboundToken: env[REST_ENV.OUTBOUND_TOKEN]!,
    trustedHeaderName: env[REST_ENV.TRUSTED_HEADER_NAME]!,
    trustedHeaderValue: env[REST_ENV.TRUSTED_HEADER_VALUE]!,
    devBearer: env[REST_ENV.DEV_BEARER] ?? null,
    displayName: env[REST_ENV.DISPLAY_NAME],
  })
}

registerChannelAdapter('rest', { factory: restFactory })
