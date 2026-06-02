/**
 * REST channel adapter for NanoClaw.
 *
 */
import { randomBytes } from 'node:crypto'
import http from 'node:http'
import https from 'node:https'

import { readEnvFile } from '../env.js'
import { log } from '../log.js'
// Resolves an ask_question card's render (title + options) by questionId. In
// nanopod this is a stub returning undefined; when rest.ts is synced into the
// VM's nanoclaw the same relative import resolves to the real SQL-backed store
// (pending_questions). Used by POST /action to decode a button index → value.
import { getAskQuestionRender } from '../db/sessions.js'
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
//
// `content` may be a structured object (evolved wire: `{ text, attachments? }`,
// attachments inline base64) or a bare non-empty string (legacy server). `kind`
// is optional and rides through to the nanoclaw InboundMessage.
interface InboundBody {
  messageId: string
  content: string | Record<string, unknown>
  kind?: string
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
  const contentIsString = typeof obj.content === 'string'
  const contentIsObject = typeof obj.content === 'object' && obj.content !== null && !Array.isArray(obj.content)
  if (contentIsString) {
    if ((obj.content as string).length === 0) {
      return { ok: false, error: 'content: must be a non-empty string' }
    }
  } else if (!contentIsObject) {
    return { ok: false, error: 'content: must be a string or object' }
  }
  if (obj.kind !== undefined && typeof obj.kind !== 'string') {
    return { ok: false, error: 'kind: must be a string' }
  }
  let replyContext: Record<string, unknown> | undefined
  if (obj.replyContext !== undefined) {
    if (typeof obj.replyContext !== 'object' || obj.replyContext === null || Array.isArray(obj.replyContext)) {
      return { ok: false, error: 'replyContext: must be an object' }
    }
    replyContext = obj.replyContext as Record<string, unknown>
  }
  return {
    ok: true,
    data: {
      messageId: obj.messageId,
      content: obj.content as string | Record<string, unknown>,
      kind: obj.kind as string | undefined,
      replyContext,
    },
  }
}

// Pull the platform message id out of the relay's outbound-POST response
// (`{ platformMessageId }`). Tolerant: any parse/shape problem yields undefined
// so `deliver` falls back to the synthetic id rather than throwing.
function parsePlatformMessageId(respBody: string): string | undefined {
  if (!respBody) return undefined
  try {
    const parsed = JSON.parse(respBody) as { platformMessageId?: unknown }
    return typeof parsed.platformMessageId === 'string' && parsed.platformMessageId.length > 0
      ? parsed.platformMessageId
      : undefined
  } catch {
    return undefined
  }
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

// Whether an outbound message carries anything worth POSTing. A bare empty
// chat (`{ text: '' }` with no files) is dropped; a file-only message, or a
// content object with any other non-empty field (markdown, type, emoji,
// options…), is delivered.
function hasOutboundPayload(content: unknown, fileCount: number): boolean {
  if (fileCount > 0) return true
  if (typeof content === 'string') return content.length > 0
  if (content !== null && typeof content === 'object') {
    return Object.values(content).some((v) => (typeof v === 'string' ? v.length > 0 : v != null))
  }
  return false
}

// `/action` body — a button click forwarded from the nanopod relay. `userId`
// is informational (the clicker's platform id) and tolerated empty.
interface ActionBody {
  questionId: string
  selectedOption: string
  userId: string
}

function validateActionBody(parsed: unknown): { ok: true; data: ActionBody } | { ok: false; error: string } {
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { ok: false, error: 'body must be a JSON object' }
  }
  const obj = parsed as Record<string, unknown>
  if (typeof obj.questionId !== 'string' || obj.questionId.length === 0) {
    return { ok: false, error: 'questionId: must be a non-empty string' }
  }
  if (typeof obj.selectedOption !== 'string' || obj.selectedOption.length === 0) {
    return { ok: false, error: 'selectedOption: must be a non-empty string' }
  }
  if (obj.userId !== undefined && typeof obj.userId !== 'string') {
    return { ok: false, error: 'userId: must be a string' }
  }
  return {
    ok: true,
    data: {
      questionId: obj.questionId,
      selectedOption: obj.selectedOption,
      userId: typeof obj.userId === 'string' ? obj.userId : '',
    },
  }
}

/**
 * Decode the option value from a button-click `selectedOption`. nanopod's Chat
 * SDK bridge encodes buttons by option INDEX (`ncq:<questionId>:<idx>`, kept
 * short for Telegram's 64-byte callback_data cap), so the value arriving over
 * `/action` is that index. Resolve it against the ask_question render — the
 * same logic the native chat-sdk-bridge runs in-process. In nanopod the stubbed
 * `getAskQuestionRender` returns `undefined` and we pass the value through; in
 * the deployed VM the real SQL store (pending_questions) resolves it to the
 * option's value. A non-numeric value (old-format card, or already a value)
 * passes through unchanged.
 */
export function resolveSelectedOption(
  render: { options: { value: string }[] } | undefined,
  selectedOption: string,
): string {
  if (render && /^\d+$/.test(selectedOption)) {
    const idx = Number(selectedOption)
    if (render.options[idx]) return render.options[idx].value
  }
  return selectedOption
}

async function postJson(url: string, body: string, token: string): Promise<{ status: number; body: string }> {
  const u = new URL(url)
  const requester = u.protocol === 'https:' ? https : http
  return new Promise((resolve, reject) => {
    const req = requester.request(
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
        // Read the response body — the relay echoes the platform message id
        // (`{ platformMessageId }`) so `deliver` can return the real id for the
        // agent's `delivered` table instead of the local placeholder.
        const chunks: Buffer[] = []
        res.on('data', (c: Buffer) => chunks.push(c))
        res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') }))
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

  // Auth shared by both POST routes (/message, /action): the trusted header is
  // authoritative when present; the dev bearer is consulted only when it's
  // absent. Returns true when authorized; otherwise sends a 401 and returns
  // false so the caller can bail.
  function authorize(req: http.IncomingMessage, res: http.ServerResponse): boolean {
    const trustedHeaderKey = opts.trustedHeaderName.toLowerCase()
    const presentedValue = req.headers[trustedHeaderKey]
    if (typeof presentedValue === 'string') {
      if (presentedValue !== opts.trustedHeaderValue) {
        log.warn('rest inbound: trusted header mismatch — 401', { header: opts.trustedHeaderName })
        sendJson(res, 401, { error: 'Unauthorized' })
        return false
      }
      log.info('rest inbound: trusted header matched')
      return true
    }
    if (opts.devBearer != null) {
      const auth = req.headers['authorization']
      if (typeof auth === 'string' && auth === `Bearer ${opts.devBearer}`) {
        log.info('rest inbound: accepted via dev bearer')
        return true
      }
      log.warn('rest inbound: missing trusted header and invalid/absent dev bearer — 401')
      sendJson(res, 401, { error: 'Unauthorized' })
      return false
    }
    log.warn('rest inbound: missing trusted header, no dev bearer configured — 401')
    sendJson(res, 401, { error: 'Unauthorized' })
    return false
  }

  // POST /message — a chat/structured message from the relay → nanoclaw inbound.
  async function handleMessage(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
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

    // Structured content carries `text` + optional inline `attachments`
    // (base64, opaque — nanoclaw decodes them). A legacy bare string
    // collapses to `{ text }`. Either way the identity is forced to the
    // bot identity below.
    const text = typeof body.content === 'string' ? body.content : (extractText(body.content) ?? '')
    const attachments =
      typeof body.content === 'object' && Array.isArray(body.content.attachments) ? body.content.attachments : undefined
    // Reply context (the message the user replied to): the relay carries it on
    // the structured `content.replyTo` ({ id?, text, sender }). Pass it through
    // opaquely — nanoclaw's formatter renders `replyTo` as a `<quoted_message>`
    // (and `reply_to=` when `id` is present). Dropped for bare-string content.
    const replyTo =
      typeof body.content === 'object' &&
      body.content.replyTo !== null &&
      typeof body.content.replyTo === 'object' &&
      !Array.isArray(body.content.replyTo)
        ? body.content.replyTo
        : undefined

    const inbound: InboundMessage = {
      id: body.messageId,
      // `chat-sdk` selects nanoclaw's rich inbound path; anything else
      // (incl. legacy/no kind) is a plain chat message.
      kind: body.kind === 'chat-sdk' ? 'chat-sdk' : 'chat',
      // `senderId: 'nanopod'` (no colon) — nanoclaw's permissions module
      // resolves this as `${channelType}:${rawHandle}` = `rest:nanopod`,
      // matching the user registered at provisioning time. The relay is the
      // wire conduit, not the user; for the single-conversation invariant
      // the user identity collapses to the bot identity.
      content: {
        text,
        sender: 'nanopod',
        senderId: 'nanopod',
        ...(attachments ? { attachments } : {}),
        ...(replyTo ? { replyTo } : {}),
      },
      timestamp: new Date().toISOString(),
    }

    log.info('rest inbound: routing message', { messageId: body.messageId, contentLen: text.length })
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
  }

  // POST /action — a card button click forwarded from the relay. Decode the
  // button index → option value against the VM's own ask_question render, then
  // hand it to nanoclaw's onAction (which writes the question_response and wakes
  // the agent). The relay carries only the index; the value lives here in the VM.
  async function handleAction(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const raw = await readBody(req)
    let parsed: unknown
    try {
      parsed = JSON.parse(raw)
    } catch {
      sendJson(res, 400, { error: 'Invalid JSON' })
      return
    }
    const result = validateActionBody(parsed)
    if (!result.ok) {
      sendJson(res, 400, { error: result.error })
      return
    }
    const { questionId, selectedOption, userId } = result.data

    const render = getAskQuestionRender(questionId)
    const value = resolveSelectedOption(render, selectedOption)
    log.info('rest inbound: routing action', { questionId, resolved: value !== selectedOption })
    // onAction is synchronous (void) in the ChannelSetup contract; guard against
    // a host throw so it can't tear down the listener.
    try {
      setupConfig?.onAction(questionId, value, userId)
    } catch (err) {
      log.error('rest inbound: onAction threw', { questionId, err: String(err) })
    }

    sendJson(res, 202, { questionId })
  }

  const setup = async (config: ChannelSetup): Promise<void> => {
    setupConfig = config
    server = http.createServer((req, res) => {
      // Wrap the async handler so unexpected throws don't tear down the server.
      void (async () => {
        try {
          const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`)
          const pathname = url.pathname
          if (pathname !== '/message' && pathname !== '/action') {
            res.statusCode = 404
            res.end()
            return
          }
          if (req.method !== 'POST') {
            res.statusCode = 405
            res.end()
            return
          }
          if (!authorize(req, res)) return

          if (pathname === '/message') {
            await handleMessage(req, res)
          } else {
            await handleAction(req, res)
          }
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
        const { status } = await postJson(opts.lifecycleReadyUrl, readyBody, opts.outboundToken)
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
    const fileCount = message.files?.length ?? 0
    if (!hasOutboundPayload(message.content, fileCount)) return undefined

    // Forward content + kind verbatim; encode each file Buffer to base64 for
    // the JSON wire (decoded back to a Buffer on the relay side).
    const files = message.files?.map((f) => ({ filename: f.filename, data: f.data.toString('base64') }))

    const messageId = `rest-out-${Date.now()}-${randomBytes(4).toString('hex')}`
    const body = JSON.stringify({
      messageId,
      kind: message.kind,
      content: message.content,
      ...(files && files.length > 0 ? { files } : {}),
      ...(lastReplyContext !== undefined ? { replyContext: lastReplyContext } : {}),
    })
    try {
      const { status, body: respBody } = await postJson(opts.messagesUrl, body, opts.outboundToken)
      if (status >= 200 && status < 300) {
        // Prefer the platform (Discord) message id the relay echoes back: that's
        // the id the agent must target to react to / edit THIS message later.
        // nanoclaw records our return value in `delivered.platform_message_id`,
        // so returning the synthetic `messageId` would make self-targeted
        // reactions/edits resolve to a non-existent id (Discord 404). Fall back
        // to the synthetic id only when the relay omits the platform id.
        const platformMessageId = parsePlatformMessageId(respBody)
        return platformMessageId ?? messageId
      }
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
