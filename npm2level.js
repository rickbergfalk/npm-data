var follow = require('follow')
var async = require('async')
var latestVersion = require('./lib/latest-version.js')
var fixNulls = require('./lib/fix-nulls.js')
var db = require('./lib/db.js')

var changeCount = 0 // every time we begin to process a change, this will be incremented
var errorLimit = 20 // If this many errors happen, we'll stop persisting to postgres and quit
var errorCount = 0
var startingSeq = 0
var couchUrl = 'https://skimdb.npmjs.com/registry'
var feedCaughtUp = false // set to true once couch is all caught up
var feedPaused = false

/*  log
============================================================================= */
function log (header, details) {
  console.log(header)
  if (details) {
    var detailsText = ''
    if (typeof details === 'string') {
      detailsText = details
    } else if (details) {
      detailsText = JSON.stringify(details, null, 2)
    }
    console.log(detailsText)
  }
}

/*  Figure out where we left off
============================================================================= */
db.misc.get('maxseq', function (err, value) {
  if (err) {
    console.log('no previous sequence found.')
  } else {
    startingSeq = value
  }
  followCouch()
})

/*  Starts following the CouchDB
============================================================================= */
function followCouch () {
  log('Starting on sequence ' + startingSeq)
  log('Use ctrl-c at any time to stop the feed processing.')

  var opts = {
    db: couchUrl,
    since: startingSeq,
    include_docs: true,
    heartbeat: 60 * 1000 // ms in which couch must responds
  }
  var feed = new follow.Feed(opts)

  // create a queue object with concurrency of n
  var q = async.queue(function (change, callback) {
    // if the queue has reached lower limit, resume feed
    var length = q.length()
    if (length < 100 && feedPaused) {
      console.log('q.length is ' + length + '. resuming feed')
      feed.resume()
      feedPaused = false
    }
    // process the change
    if (!change.doc) console.log('%s   %s   %s', change.seq, change.id, (change.doc ? '' : '(NO DOC!)'))
    processChangeDoc(change, callback)
  }, 4)

  // on a change doc received push to the q
  feed.on('change', function (change) {
    // If the queue is big pause the feed
    var length = q.length()
    if (length > 5000 && !feedPaused) {
      console.log('q.length is ' + length + '. pausing feed')
      feed.pause()
      feedPaused = true
    }
    // add change to the queue
    q.push(change, function (err) {
      // we aren't returning errors here so...
      if (err) console.log(err)
    })
  })

  // If feed has an error just die
  feed.on('error', function (err) {
    log('follow feed error', err)
    console.error('Follow Feed Error: Since Follow always retries, this must be serious')
    throw err
  })

  feed.on('confirm', function (db) {
    log('npm db confirmed... Starting feed processing.')
  })

  feed.on('catchup', function (seqId) {
    feedCaughtUp = true
    feed.stop()
    log('Packages from CouchDB caught up. Last sequence: ' + seqId)
  })

  // once q is finished, if feed is stopped continue on to  next thing
  q.drain = function () {
    if (feedCaughtUp) {
      log('Exiting npm2pg')
      process.exit()
    }
  }

  feed.follow()
}

/*  processChangeDoc

  This will be called for every change from couchdb.

  NOTE - here's what a change doc looks like for a deleted item
  {
    seq: 311059,
    id: 'Fadecandy-Client',
    changes: [ { rev: '2-17340ec3b13a7a8d5369975a2cf4eec4' } ],
    deleted: true,
    doc: {
      _id: 'Fadecandy-Client',
      _rev: '2-17340ec3b13a7a8d5369975a2cf4eec4',
      _deleted: true
    }
  }
============================================================================= */
var metricsBeginTime
var metricsEndTime

function processChangeDoc (change, cb) {
  async.waterfall([
    function initData (next) {
      fixNulls(change.doc)
      if (!change.doc) {
        return next('no doc?', change)
      }
      if (change.doc.versions) {
        // versions maybe not available for deleted things
        var versionsArray = Object.keys(change.doc.versions)
        var latestVer = latestVersion(Object.keys(change.doc.versions))
        change.doc.latestVersion = change.doc.versions[latestVer]
        change.doc.versions = {}
        versionsArray.forEach(function (v) {
          change.doc.versions[v] = {}
        })
      }
      next(null, change)
    },
    function updateOrInsertPackage (change, next) {
      db.package.put(change.doc._id, change.doc, function (err) {
        if (err) return next(err, change)
        next(err, change)
      })
    },
    function logProgress (change, next) {
      changeCount++
      if (!metricsBeginTime) metricsBeginTime = new Date()

      // Every 1000 changes we should log something interesting to look at.
      // like change count and inserts per second, for fun
      if (changeCount % 1000 === 0) {
        metricsEndTime = new Date()
        var seconds = (metricsEndTime - metricsBeginTime) / 1000
        var packagesPerSecond = Math.round(1000 / seconds)
        log('Packages processed: ' + changeCount + '     Packages/sec: ' + packagesPerSecond)
        // reset timers
        metricsBeginTime = null
        metricsEndTime = null

        db.misc.put('maxseq', change.seq, function (err) {
          if (err) {
            console.error('Error writing maxseq to db.misc')
            console.error(err)
          }
        })
      }

      // proceed to next step
      next(null, change)
    }
  ], function theEndOfProcessingAChange (err, change) {
    if (err) {
      change.err = err
      log('An insert/update failed - ' + change.id, change)
      errorCount++
    }
    if (errorCount < errorLimit) {
      cb()
    } else {
      console.log('Reached error limit (' + errorLimit + '). Stopping this thing.')
      process.exit()
    }
  })
}
