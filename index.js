const fs = require('fs')
const http = require('http')
const https = require('https')
const url = require('url')
const path = require('path')
const $rdf = require('rdflib')
const hypergraph = require('hyper-graph-db')
const Progress = require('progress')
const md5 = require('md5')
const sha1 = require('node-sha1')

// Links of test data:
const online_cco = {
  name: 'CCO',
  url: 'http://data.bioontology.org/ontologies/CCO/submissions/9/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb'
}

const online_cco_09 = {
  name: 'CCO_09.rdf',
  url: 'http://data.bioontology.org/ontologies/CCO/submissions/8/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb'
}

const online_dw = {
  name: 'Dat_Whitepaper',
  url: 'https://github.com/datprotocol/whitepaper/raw/master/dat-paper.pdf'
}

// directories:
const ressourcesDir = './data/ressources'
const graphDir = './data/graphDBs/hyper-graph-db'

main()

async function main () {
  const rawData = await getRessources(ressourcesDir, [online_cco])
  console.log('rawData', typeof rawData[0])
  const graphDB = await getHypergraph(graphDir)
  ressourceIngestion(graphDB, rawData[0])
}

async function getHypergraph (graphDir) {
  let keyFilePath = path.join(graphDir, 'key')
  let key
  try {
    key = await fs.readFileSync(keyFilePath).toString()
    key = Buffer.from(key, 'hex')
  } catch (err) {
    key = null
  }

  if (key) return hypergraph(graphDir, key, { valueEncoding: 'utf-8' })

  let newGraphDB = hypergraph(graphDir, { valueEncoding: 'utf-8' })
  newGraphDB.ready(() => {
    fs.writeFile(keyFilePath,
        newGraphDB.db.key.toString('hex'), (err) => console.warn(err)
      )
  })
  return graphDB
}

async function ressourceIngestion (graphDB, rawData) {
  let rdfBlocks = rawData.split('\n\n')

  let rdfHead = rdfBlocks.shift()
  let rdfTailFragments = rdfBlocks.pop().split('\n')
  let rdfTail = rdfTailFragments.pop()
  rdfBlocks.push(rdfTailFragments.reduce((sum, elem) => sum + elem + '\n'))
  // console.log('rdfHead:', rdfHead)
  // console.log('rdfTail', rdfTail)
  // console.log('lastElem:', rdfBlocks[rdfBlocks.length - 1])

  let contentType = 'application/rdf+xml'
  let baseUrl = "http://data.bioontology.org/ontologies/CCO"

  let i = 0
  let step = 100
  let length = rdfBlocks.length

  while (i < length) {
    let j = i + step
    let store = $rdf.graph();

    console.log(i,j,length)
    if (j > length) {
      j = length - 1
      if (i===j) break
    }
    console.log(i,j,length)
    
    let parseString = rdfHead + '\n\n' + rdfBlocks.slice(i, j).reduce((sum, elem) => sum + elem + '\n\n') + rdfTail
    try {
      $rdf.parse(parseString, store, baseUrl, contentType)
    } catch (err) {
      console.warn(err)
    }

    let stms = store.statementsMatching(undefined, undefined, undefined)
    console.log('typeof stms:', typeof stms[0], stms[0], stms.length)
    let triples = stms.map(elem => ({ subject: elem.subject.value, object: elem.object.value, predicate: elem.predicate.value }))
    console.log('triples', triples[0])
    graphDB.put(triples)

    i = j
  }

  graphDB.get({ predicate: undefined, subject: undefined, object: undefined }, (err, res) => {
    if (err) console.warn(err)
    console.log(res)
  })

  // let stms = store.statementsMatching(undefined, undefined, undefined)
  // console.log(stms.length)
  // for (let i=0; i < 25; i++) {
  //   console.log(`Statement ${i}: ${stms[i]}`)
  // }
  return
}

async function getRessources (ressourcesDir, onlineRessources) {
  const checkSums = await getCheckSums(ressourcesDir)
  let existingRessources
  if (checkSums) {
    existingRessources = await checkFiles(onlineRessources, { checkSums })
  }
  for (index in onlineRessources) {
    if (!existingRessources[index]) {
      download(ressourcesDir, onlineRessources[index])
    }
  }
  let ret = []
  for (ressource of onlineRessources) {
    ret.push(fs.readFileSync(path.join(ressourcesDir, ressource.name), { encoding: 'utf-8' }).toString())
  }
  return Promise.all(ret)
}

function download (dataDir, online) {
  let ws = fs.createWriteStream(path.join(dataDir, online.name))

  let req = http.request(getUrlOptions(online.url, 'GET'), (res, socket, err) => {
    if (err) console.warn('err', err)
    res.setEncoding('utf-8')
    res.on('end', () => console.log('Download finished!'))
    res.pipe(ws)
  })

  req.on('error', err => console.log('err:', err))

  // Show download progress
  req.on('response', (res => {
    let contentSize = Number(res.headers['content-length'])
    console.log(`downloading ${contentSize / 1024 / 1024} MiB`)
    const progressBar = new Progress('downloading [:bar] :rate/bps :percent :etas',
      { complete: '=', incomplete: ' ', width: 20, total: contentSize }
    )
    res.on('data', chunk => {
      progressBar.tick(chunk.length)
    })
  }))
  req.end()

  return
}

async function checkFiles (ressources, localInfo) {
  let { checkSums, stats } = localInfo

  if (!checkSums) return null // maybe implement comparison based on stats info

  let ret = []
  for (ressource of ressources) {
    let resCheckSum = checkSums[ressource.name]
    if (!resCheckSum) {
      ret.push(false)
      continue
    }
    console.log('Awaiting remote ressource info')
    ret.push(new Promise((resolve, reject) => {
      let req = http.request(getUrlOptions(ressource.url, "HEAD"), res => {
        if (res.headers['x-content-digest'] === resCheckSum.sha1) resolve(true)
        if (res.headers['x-content-digest'] === resCheckSum.md5) resolve(true)
        resolve(false)
      })
      req.on('error', (err) => {
        console.warn(err)
        reject(err)
      })
      req.end()
    }))
  }
  return Promise.all(ret)
}

async function getCheckSums(dataDir) {
  let checkSums = {}
  let files = await fs.readdirSync(dataDir)
  for (file of files) {
    fileStream = fs.createReadStream(path.join(dataDir, file))
    checkSums[file] = await new Promise((resolve, reject) => {
      sha1(fileStream, (err, hash) => {
        if (err) {
          console.warn(err)
          reject(err)
        }
        resolve({ sha1: hash })
      })
    })
  }
  return checkSums
}

async function getFileStats (dataDir) {
  let files = await fs.readdirSync(dataDir)
  let fileStats = {}
  for (file of files) {
    fileStats[file] = await fs.statSync(path.join(dataDir, file))
  }
  return fileStats
}

function getUrlOptions (url_string, method) {
  const parsedUrl = url.parse(url_string)
  return {
    hostname: parsedUrl.hostname,
    path: parsedUrl.path,
    method: method
  }
}