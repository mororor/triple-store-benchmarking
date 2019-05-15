const fs = require('fs')
const http = require('http')
const https = require('https')
const url = require('url')
const path = require('path')
const $rdf = require('rdflib')
const hypergraph = require('hyper-graph-db')
const levelgraph = require('levelgraph')
const levelBrowserify = require('level-browserify')
const Progress = require('progress')
const md5 = require('md5')
const sha1 = require('node-sha1')
const toml = require('toml')

// Links of test data:
const online_cco = {
  name: 'CCO',
  url: 'http://data.bioontology.org/ontologies/CCO/submissions/9/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb'
}

const online_cco_09 = {
  name: 'CCO_09.rdf',
  url: 'http://data.bioontology.org/ontologies/CCO/submissions/8/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb'
}

// directories:
const configDir = './config.toml'
const ressourcesDir = './data/ressources'
const graphDir = './data/graphDBs/'

main()

async function main () {
  const { ressources, graphSelection } = await getConfig()
  console.log(ressources, graphSelection)
  const rawData = await getRessources(ressourcesDir, ressources)
  const graphs = await getGraphs(graphSelection)
  // const graphDB = await getHypergraph(graphDir)
  for (graph of Object.keys(graphs)) {
    // console.log(graph)
    // console.log(graphs[graph])
    ressourceIngestion(graphs[graph], rawData[0])
  }
}

async function getConfig () {
  let confToml = await fs.readFileSync(configDir).toString()
  let { ressources, graphDBs: graphs } = toml.parse(confToml)
  processedRessources = []
  for (entry of Object.keys(ressources)) {
    processedRessources.push(ressources[entry])
  }
  return { ressources: processedRessources, graphSelection: graphs }
}

async function getGraphs (graphSelection) {
  console.log(graphSelection)
  for (key of Object.keys(graphSelection)) {
    if (!graphSelection[key]) continue
    switch (key) {
      case 'hyper-graph-db':
        graphSelection[key] = await getHypergraph(path.join(graphDir, key))
        break
      case 'LevelGraph':
        graphSelection[key] = await getLevelGraph(path.join(graphDir, key))
        break
      default:
        continue
    }
  }
  return graphSelection
}

async function getLevelGraph (graphDir) {
  return levelgraph(levelBrowserify(graphDir))
}

async function getHypergraph (graphDir) {
  let keyFilePath = path.join(graphDir, 'key')
  let key
  try {
    key = await fs.readFileSync(keyFilePath).toString()
    console.log('key:', key)
    if (key !== '') key = Buffer.from(key, 'hex')
    else key = null
  } catch (err) {
    key = null
  }
  
  if (key) return hypergraph(graphDir, key, { valueEncoding: 'utf-8' })

  let newGraphDB = hypergraph(graphDir, { valueEncoding: 'utf-8' })
  newGraphDB.ready(() => {
    console.log(newGraphDB.db.key.toString('hex'))
    fs.writeFile(keyFilePath,
        newGraphDB.db.key.toString('hex'), (err) => console.warn(err)
      )
  })
  return newGraphDB
}

async function ressourceIngestion (graphDB, rawData) {
  await graphDB
  console.log('graphDB:', graphDB)
  let rdfBlocks = rawData.split('\n\n')
  rdfBlocks.pop()
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
  let step = 1000
  let length = rdfBlocks.length

  while (i < length) {
    let j = i + step
    let store = $rdf.graph();

    // console.log(i,j,length)
    if (j >= length) {
      j = length - 1
      if (i===j) break
    }
    // console.log(i,j,length)
    
    let parseString = rdfHead + '\n\n' + rdfBlocks.slice(i, j).reduce((sum, elem) => sum + elem + '\n\n') + rdfTail
    // console.log('parseString', parseString)
    try {
      $rdf.parse(parseString, store, baseUrl, contentType)
    } catch (err) {
      console.warn(err)
    }

    let stms = store.statementsMatching(undefined, undefined, undefined)
    // console.log('typeof stms:', typeof stms)
    // console.log('typeof stms[0]:', typeof stms[0])
    // console.log('stms[0]:', stms[0])
    // console.log('stms.length:', stms.length)
    let triples = stms.map(elem => ({ subject: elem.subject.value, object: elem.object.value, predicate: elem.predicate.value }))
    // console.log('triples[0]', triples[0])
    // console.log('triples', triples)
    store.close()
    await putToGraphAsync(graphDB, triples).catch('error here')

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

async function putToGraphAsync (graph, triples) {
  return new Promise((resolve, reject) => graph.put(triples, (err, res) => {
    if (err) {
      console.warn(err)
      reject(err)
    }
    resolve(res) 
  }))
}

async function getRessources (ressourcesDir, onlineRessources, doNotUpdate) {
  doNotUpdate = true
  if (!doNotUpdate) {
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