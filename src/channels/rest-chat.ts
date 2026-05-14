#!/usr/bin/env tsx
/**
 * Smoke-test CLI for the REST channel adapter.
 *
 * Usage:  tsx rest-chat.ts <host:port> "<message>" [options]
 *
 * Options:
 *   --bearer <token>         inbound auth token (default: REST_DEV_BEARER from .env)
 *   --timeout <ms>           reply timeout in ms (default: 30000)
 *   --wait-for-ready <secs>  wait up to N seconds for rest.ts lifecycle/ready POST
 *                            before sending the test message (start this process
 *                            BEFORE launching nanoclaw so it is ready to receive it)
 *
 * Reads REST_MESSAGES_URL and REST_LIFECYCLE_READY_URL from .env to determine
 * which port to bind and which paths to handle.
 *
 * Exit 0 — received a non-empty reply.
 * Exit 1 — timeout, HTTP error, or missing config.
 */
import http from 'node:http'
import { randomBytes } from 'node:crypto'
import { readEnvFile } from '../env.js'

function usage(): never {
  process.stderr.write(
    'usage: tsx rest-chat.ts <host:port> "<message>" [--bearer <token>] [--timeout <ms>] [--wait-for-ready <secs>]\n',
  )
  process.exit(1)
}

function parseArgs() {
  const args = process.argv.slice(2)
  const positional: string[] = []
  let bearer: string | undefined
  let timeoutMs = 30_000
  let waitForReadySecs: number | undefined

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--bearer') {
      bearer = args[++i]
    } else if (args[i] === '--timeout') {
      timeoutMs = parseInt(args[++i] ?? '', 10)
    } else if (args[i] === '--wait-for-ready') {
      waitForReadySecs = parseInt(args[++i] ?? '', 10)
    } else {
      positional.push(args[i])
    }
  }

  if (positional.length < 2) usage()
  return { target: positional[0], message: positional[1], bearer, timeoutMs, waitForReadySecs }
}

async function readBody(req: http.IncomingMessage): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(chunk as Buffer)
  return Buffer.concat(chunks).toString('utf8')
}

async function postJson(hostname: string, port: number, path: string, body: string, bearer: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname,
        port,
        path,
        method: 'POST',
        headers: {
          Authorization: `Bearer ${bearer}`,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      },
      (res) => {
        res.on('data', () => {})
        res.on('end', () => resolve(res.statusCode ?? 0))
      },
    )
    req.on('error', reject)
    req.write(body)
    req.end()
  })
}

async function main() {
  const { target, message, bearer: bearerArg, timeoutMs, waitForReadySecs } = parseArgs()

  const env = readEnvFile(['REST_DEV_BEARER', 'REST_MESSAGES_URL', 'REST_LIFECYCLE_READY_URL'])

  const bearer = bearerArg ?? env['REST_DEV_BEARER']
  if (!bearer) {
    process.stderr.write('error: no bearer token — pass --bearer or set REST_DEV_BEARER in .env\n')
    process.exit(1)
  }

  const messagesUrl = env['REST_MESSAGES_URL']
  if (!messagesUrl) {
    process.stderr.write('error: REST_MESSAGES_URL not set in .env\n')
    process.exit(1)
  }

  const parsedMessages = new URL(messagesUrl)
  const listenPort = parseInt(parsedMessages.port || '80', 10)
  const messagesPath = parsedMessages.pathname

  let readyPath: string | undefined
  if (waitForReadySecs !== undefined) {
    const readyUrl = env['REST_LIFECYCLE_READY_URL']
    if (!readyUrl) {
      process.stderr.write('error: --wait-for-ready requires REST_LIFECYCLE_READY_URL in .env\n')
      process.exit(1)
    }
    readyPath = new URL(readyUrl).pathname
  }

  let resolveReply!: (text: string) => void
  let resolveReady!: () => void
  const replyPromise = new Promise<string>((res) => {
    resolveReply = res
  })
  const readyPromise = new Promise<void>((res) => {
    resolveReady = res
  })

  const server = http.createServer((req, res) => {
    void (async () => {
      const raw = await readBody(req)
      res.statusCode = 200
      res.end()

      if (req.url === readyPath && waitForReadySecs !== undefined) {
        process.stderr.write('rest-chat: received lifecycle/ready signal\n')
        resolveReady()
        return
      }

      if (req.url === messagesPath) {
        try {
          const body = JSON.parse(raw) as Record<string, unknown>
          const content = body['content']
          resolveReply(typeof content === 'string' ? content : JSON.stringify(content))
        } catch {
          resolveReply(raw)
        }
      }
    })()
  })

  await new Promise<void>((res) => server.listen(listenPort, '0.0.0.0', res))
  process.stderr.write(
    `rest-chat: listening on :${listenPort} (messages=${messagesPath}${readyPath ? `, ready=${readyPath}` : ''})\n`,
  )

  // Optionally wait for the REST adapter's lifecycle/ready signal.
  if (waitForReadySecs !== undefined) {
    process.stderr.write(`rest-chat: waiting up to ${waitForReadySecs}s for ready signal...\n`)
    const readyTimeout = new Promise<'timeout'>((res) => setTimeout(() => res('timeout'), waitForReadySecs * 1000))
    const result = await Promise.race([readyPromise.then(() => 'ok' as const), readyTimeout])
    if (result === 'timeout') {
      process.stderr.write('error: timed out waiting for lifecycle/ready signal\n')
      server.close()
      process.exit(1)
    }
  }

  // Send inbound message to the adapter.
  const [host, portStr] = target.split(':')
  const adapterPort = portStr ? parseInt(portStr, 10) : 8000
  const messageId = `smoke-${Date.now()}-${randomBytes(4).toString('hex')}`
  const body = JSON.stringify({ messageId, content: message })

  let sendStatus: number
  try {
    sendStatus = await postJson(host, adapterPort, '/message', body, bearer)
  } catch (err) {
    process.stderr.write(`error: POST /message failed — ${String(err)}\n`)
    server.close()
    process.exit(1)
  }

  if (sendStatus < 200 || sendStatus >= 300) {
    process.stderr.write(`error: POST /message returned HTTP ${sendStatus}\n`)
    server.close()
    process.exit(1)
  }

  process.stderr.write(`rest-chat: message sent (id=${messageId}), waiting for reply...\n`)

  const timeoutHandle = setTimeout(() => {
    process.stderr.write(`error: timed out after ${timeoutMs}ms — no reply received\n`)
    server.close()
    process.exit(1)
  }, timeoutMs)

  const reply = await replyPromise
  clearTimeout(timeoutHandle)
  server.close()

  process.stdout.write(`${reply}\n`)
  process.exit(0)
}

main().catch((err) => {
  process.stderr.write(`fatal: ${String(err)}\n`)
  process.exit(1)
})
