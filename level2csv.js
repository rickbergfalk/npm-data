var fs = require('fs')
var semver = require('semver')
var csv = require('fast-csv')
var db = require('./lib/db.js')
var log = false // turn on for logging

/*  set up csv/file streams
============================================================================= */
var csvStreams = {}
function addCsvStream (headers, quoteColumns, filenameSansExtension) {
  var csvStream = csv.createWriteStream({headers: headers, quoteColumns: quoteColumns})
  var writableStream = fs.createWriteStream('./csv/' + filenameSansExtension + '.csv')
  csvStreams[filenameSansExtension] = csvStream
  writableStream.on('finish', function () {
    console.log(filenameSansExtension + ' writable stream has finished')
  })
  csvStream.pipe(writableStream)
}
var packageHeaders = [
  'package_name',
  'latest_version',
  'readme',
  'created_date',
  'modified_date',
  'description',
  'repository',
  'license',
  'homepage',
  'author_name',
  'author_email'
]
addCsvStream(packageHeaders, true, 'packages')
var versionHeaders = ['package_name', 'version', 'version_seq', 'time', 'major', 'minor', 'patch', 'prerelease', 'publish_type']
addCsvStream(versionHeaders, false, 'versions')
addCsvStream(['package_name', 'dependency_name'], false, 'dependencies')
addCsvStream(['package_name', 'dependency_name'], false, 'dev_dependencies')
addCsvStream(['package_name', 'keyword'], false, 'keywords')
addCsvStream(['package_name', 'rejected_reason', 'doc'], true, 'rejected_packages')
// addCsvStream(['package_name', 'doc'], true, 'package_doc');

/*  Stream results from leveldb
    {gte: 'package~', lte: 'package~~'}
============================================================================= */
var count = 0
var start = new Date()
db.package.createReadStream()
    .on('data', function (data) {
      count++
      if (count % 10000 === 0) {
        console.log(count + ' ' + data.value._id) // data.value for doc
      }
      transformData(data.value)
    })
    .on('error', function (err) {
      console.log('Oh my!', err)
    })
    .on('close', function () {
      // console.log('Stream closed')
    })
    .on('end', function () {
      console.log('Stream closed')
      console.log('Total count: ' + count)
      var end = new Date()
      var timeTaken = ((end - start) / 1000).toFixed(1)
      console.log('seconds: ', timeTaken)
      console.log('minutes: ', (timeTaken / 60).toFixed(1))

      // TODO where should this go?
      for (var key in csvStreams) {
        csvStreams[key].end()
      }
    })

/*  Transform the data for each package
============================================================================= */
function transformData (doc) {
  /*
  csvStreams['package_doc'].write({
      package_name: doc._id,
      doc: JSON.stringify(doc)
  });
  */

  if (doc._deleted) {
    csvStreams['rejected_packages'].write({
      package_name: doc._id,
      rejected_reason: 'deleted',
      doc: JSON.stringify(doc)
    })
    return
  }
  if (!doc.versions) {
    csvStreams['rejected_packages'].write({
      package_name: doc._id,
      rejected_reason: 'missing versions',
      doc: JSON.stringify(doc)
    })
    return
  }

  // package
  var createdDate = (doc.time ? doc.time.created : doc.ctime)
  var modifiedDate = (doc.time ? doc.time.modified : doc.mtime)
  if (!createdDate) {
    csvStreams['rejected_packages'].write({
      package_name: doc._id,
      rejected_reason: 'missing created date',
      doc: JSON.stringify(doc)
    })
    return
  }

  var sortedVersions = Object.keys(doc.versions).filter(function (v) {
    return semver.valid(v)
  }).sort(semver.compare)
  if (log) console.log(sortedVersions)
  var versions = []
  var previousStable = {}
  for (var i = 0; i < sortedVersions.length; i++) {
    var version = {
      package_name: doc._id,
      version_seq: i + 1,
      time: (doc.time ? doc.time[sortedVersions[i]] : null),
      version: sortedVersions[i],
      major: semver.major(sortedVersions[i], true),
      minor: semver.minor(sortedVersions[i], true),
      patch: semver.patch(sortedVersions[i], true),
      prerelease: semver.prerelease(sortedVersions[i], true),
      publish_type: null // dev, major, minor, patch, prerelease
    }
    if (version.prerelease) {
      version.prerelease = version.prerelease.join('.')
      version.publish_type = 'prerelease'
    }

    // if there's a previousStable version object, lets compare and figure out
    // what kind of publish type this was (major, minor, patch)
    if (!version.prerelease) {
      if (version.major === 0) version.publish_type = 'dev'
      else if (version.major > 0 && !previousStable) version.publish_type = 'major'
      else if (version.major !== previousStable.major) version.publish_type = 'major'
      else if (version.major === previousStable.major && version.minor !== previousStable.minor) version.publish_type = 'minor'
      else if (version.major === previousStable.major && version.minor === previousStable.minor && version.patch !== previousStable.patch) version.publish_type = 'patch'
      else version.publish_type = 'unknown'
      // record the current version as the previousStable for next round
      previousStable = version
    }
    versions.push(version)
  }
  if (log) console.log(versions)
  versions.forEach(function (v) {
    csvStreams['versions'].write(v)
  })

  // latest version doc
  var latestVersionDoc = doc.latestVersion
  if (!latestVersionDoc) {
    latestVersionDoc = {}
  }
  if (log) console.log(latestVersionDoc)

  // latest version dependencies
  var dependencies = []
  if (latestVersionDoc.dependencies) {
    dependencies = Object.keys(latestVersionDoc.dependencies).map(function (dep) {
      return {
        package_name: doc._id,
        dependency_name: dep
      }
    })
  }
  dependencies.forEach(function (dep) {
    csvStreams['dependencies'].write(dep)
  })
  if (log) console.log(dependencies)

  // latest version devdependencies
  var devDependencies = []
  if (latestVersionDoc.devDependencies) {
    devDependencies = Object.keys(latestVersionDoc.devDependencies).map(function (dep) {
      return {
        package_name: doc._id,
        dependency_name: dep
      }
    })
  }
  devDependencies.forEach(function (dep) {
    csvStreams['dev_dependencies'].write(dep)
  })
  if (log) console.log(devDependencies)

  // latest keywords
  var keywords = []
  if (latestVersionDoc.keywords && latestVersionDoc.keywords.map) {
    keywords = latestVersionDoc.keywords.map(function (kw) {
      return {
        package_name: doc._id,
        keyword: kw
      }
    })
  }
  keywords.forEach(function (keyword) {
    csvStreams['keywords'].write(keyword)
  })
  if (log) console.log(keywords)

  // package csv
  var pkg = {
    package_name: doc._id,
    latest_version: latestVersionDoc.version,
    readme: doc.readme,
    created_date: createdDate,
    modified_date: modifiedDate,
    description: doc.description,
    repository: (doc.repository ? doc.repository.url : null),
    license: doc.license,
    homepage: doc.homepage,
    author_name: (doc.author ? doc.author.name : null),
    author_email: (doc.author ? doc.author.email : null)
  }
  csvStreams['packages'].write(pkg)

  // low priority (scripts, maintainers, contributors, users)
}
