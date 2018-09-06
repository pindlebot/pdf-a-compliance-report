const { spawn } = require('child_process')
const got = require('got')
const xml2js = require('xml2js')
const { randomBytes } = require('crypto')
const fs = require('fs')
const { promisify } = require('util')
const parseXml = promisify(xml2js.parseString)
const walk = require('./util/walk')
const install = require('./util/install')

async function verify (file) {
  let child = spawn('/tmp/verapdf/verapdf', [file], { shell: true })
  let result = ''
  let resolveExecution
  let rejectExecution
  let promise = new Promise((resolve, reject) => {
    resolveExecution = resolve
    rejectExecution = reject
  })
  child.stdout.on('data', data => {
    let str = data.toString('utf8')
    console.log('data', str)
    result += str
  })
  child.stderr.on('data', data => {
    let str = data.toString('utf8')
    console.log('error', str)
  })
  child.on('close', () => resolveExecution(result))
  child.on('error', rejectExecution)

  return promise
}

const download = async ({ file, pdfPath }) => {
  let _resolve
  let _reject
  let promise = new Promise((resolve, reject) => {
    _resolve = resolve
    _reject = reject
  })
  const writable = fs.createWriteStream(pdfPath)
  const stream = got.stream(file)
  stream.on('error', _reject)
  writable.on('close', _resolve)
  stream.pipe(writable)
  return promise
}

const handleError = (webhook, error) => {
  return got(webhook, {
    json: true,
    body: {
      error: error.toString()
    }
  })
}

const handleMessage = async (record) => {
  const { file, webhook } = record
  let id = randomBytes(10).toString('hex')
  let pdfPath = `/tmp/${id}.pdf`
  try {
    await download({ file, pdfPath })
  } catch (error) {
    return handleError(webhook)(error)
  }
  let out
  try {
    out = await verify(pdfPath)
  } catch (error) {
    return handleError(webhook)(error)
  }

  let validationResults
  try {
    validationResults = await parseXml(out)
  } catch (error) {
    return handleError(webhook, error)
  }

  let jobs = walk(validationResults.report.jobs)
  return got(webhook, {
    json: true,
    body: jobs[0]
  })
}

module.exports.handler = async (event, context, callback) => {
  const { Records } = event
  await install()
  await Promise.all(
    Records.map(record => {
      let { Sns: { Message } } = record
      let message = JSON.parse(Message)
      return handleMessage(message)
    })
  )
  callback(null, {})
}
