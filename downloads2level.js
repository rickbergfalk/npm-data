var fs = require('fs')
var path = require('path')
var db = require('./lib/db')
var async = require('async')
var downloadCounts = require('npm-download-counts')
var moment = require('moment')

var failedPackages = []
const LOG_FILE_PATH = path.join(__dirname, '/downloads2level.log.txt')
console.log(LOG_FILE_PATH)
fs.writeFileSync(LOG_FILE_PATH, '')

function log (words) {
  fs.appendFile(LOG_FILE_PATH, words + '\n', {encoding: 'utf8'}, function (err) {
    if (err) throw err
  })
}

var packageStream = db.package.createReadStream()
var feedPaused = false
var feedFinished = false

var q = async.queue(function (data, callback) {
  // if queue has reached lower limit resume level read stream
  if (q.length() < 400 && feedPaused) {
    // resume feed
    packageStream.resume()
    feedPaused = false
  }
  getDownloadsForPackage(data, callback)
}, 2)

packageStream.on('data', function (data) {
  if (q.length() > 2000 && !feedPaused) {
    packageStream.pause()
    feedPaused = true
  }
  if (data.value && data.value._deleted) {
    log(data.value + ' is deleted. skipping.')
    return
  }
  var qData = {
    packageName: data.key,
    timeCreated: null
  }
  if (data.value) qData.timeCreated = (data.value.time ? data.value.time.created : data.value.ctime)
  q.push(qData, function (err) {
    // we aren't returning errors here so...
    if (err) console.error(err)
  })
})
packageStream.on('error', function (err) {
  console.log('Error streaming packages: ', err)
})
packageStream.on('end', function () {
  feedFinished = true
})

q.drain = function () {
  if (feedFinished) {
    console.log('finished getting downloads')
    process.exit()
  }
}

// var data = {
//   packageName: 'sqlpad',
//   timeCreated: null,
//   lastDownloadDay: null
// }
var i = 0
function getDownloadsForPackage (data, cb) {
  i++
  if (i % 1000 === 0) {
    console.log(i + ' - ' + data.packageName)
  }
  db.downloads.get(data.packageName, function (err, value) {
    data.downloads = []
    if (err) {
      // key not found, so get all dowloads since creation
      data.startDay = moment(data.timeCreated).startOf('day')
    }
    if (value) {
      // take note of downloads and figure out start day from lastDayRequested
      if (value.downloads) data.downloads = value.downloads
      if (value.lastDayRequested) data.startDay = moment(value.lastDayRequested).add(1, 'days').startOf('day')
    }
    if (!data.startDay) {
      data.startDay = moment('01/01/2010')
    }
    data.endDay = moment().subtract(1, 'days').startOf('week')

    // if the requested start day is after requested end day return
    // this happens if we are all caught up for this package
    if (data.startDay > data.endDay) {
      // this is such a common case - only log every n times
      if (i % 100 === 0) log(`${i}   caught up.   package: ${data.packageName}   data.startDay: ${data.startDay}   data.endDay: ${data.endDay}`)
      return cb()
    }
    if (!data.startDay.isValid() || !data.endDay.isValid()) {
      console.log('Invalid date detected: ' + data.packageName)
      console.log('time created:           ' + data.timeCreated)
      if (value) console.log('value.lastDayRequested: ' + value.lastDayRequested)
      log(`Invalid date detected: ${data.packageName}`)
      return cb()
    }
    data.startDay = data.startDay.toDate()
    data.endDay = data.endDay.toDate()
    downloadCounts(data.packageName, data.startDay, data.endDay, function (err, downloads) {
      if (err && err.message.indexOf('no stats for this package for this range') === -1) {
        // maybe there was a network error or the api is down or something
        // just continue on
        log(`${i}   no stats for this range.   package: ${data.packageName}   data.startDay: ${data.startDay}   data.endDay: ${data.endDay}`)
        return cb()
      }
      var putData = {
        lastDayRequested: data.endDay,
        downloads: (downloads ? data.downloads.concat(downloads) : data.downloads)
      }
      db.downloads.put(data.packageName, putData, function (err) {
        if (err) {
          data.err = err.toString()
          console.log(data)
          failedPackages.push(data)
          if (failedPackages.length > 100) {
            console.log('TOO MANY FAILED PACKAGES')
            process.exit()
          }
        }
        log(`${i}   package: ${data.packageName}   data.startDay: ${data.startDay}   data.endDay: ${data.endDay}`)
        cb()
      })
    })
  })
}
