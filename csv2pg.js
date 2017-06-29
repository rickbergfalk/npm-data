var fs = require('fs')
var pg = require('pg')
var copyFrom = require('pg-copy-streams').from
var async = require('async')
var client = new pg.Client('postgres://npmpg:npmpg@localhost/npmpg')

client.connect(function (err) {
  if (err) throw err
  async.series([
    function streamDependencies (next) {
      streamFile('./csv/dependencies.csv', 'dependencies', client, next)
    },
    function streamDevDependencies (next) {
      streamFile('./csv/dev_dependencies.csv', 'dev_dependencies', client, next)
    },
    function streamKeywords (next) {
      streamFile('./csv/keywords.csv', 'keywords', client, next)
    },
    function streamVersions (next) {
      streamFile('./csv/versions.csv', 'versions', client, next)
    },
    function streampackages (next) {
      streamFile('./csv/packages.csv', 'packages', client, next)
    },
    function streamRejectedPackages (next) {
      streamFile('./csv/rejected_packages.csv', 'rejected_packages', client, next)
    }
    /*
    ,
    function streamPackageDoc (next) {
      streamFile('./csv/package_doc.csv', 'package_doc', client, next);
    }
    */
  ], function allDoneStreaming (err) {
    console.log('all done streaming')
    setTimeout(function () {
      client.end()
    }, 5000)
    if (err) console.error(err)
  })
})

function streamFile (filepath, table, client, callback) {
  console.log('streaming ' + filepath)
  client.query('TRUNCATE TABLE ' + table, function (err) {
    if (err) console.error(err)
    var stream = client.query(copyFrom('COPY ' + table + " FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '\"')"))
    var fileStream = fs.createReadStream(filepath)
    fileStream
      .on('error', function handleError (err) {
        console.error('filestream error')
        console.error(err)
      })
    fileStream.pipe(stream)
      .on('finish', function finish () {
        callback()
      })
      .on('error', function handleError (err) {
        console.error('filestream piping error')
        console.error(err)
      })
  })
}

/*
var Readable = require('stream').Readable
function fromReadableStream () {
  var s = new Readable
  client.connect(function (err) {
    if (err) console.log(err);
    var stream = client.query(copyFrom("COPY test FROM STDIN WITH  (FORMAT CSV, HEADER, DELIMITER ',')"));
    s.pipe(stream).on('finish', done).on('error', done);
    s.push('a,b\n')
    s.push('9,9\n')
    s.push('8,8\n')
    s.push(null)            // indicates end-of-file basically - the end of the stream
    s.on('error', handleError);
  });
}
*/
