var path = require('path')
var level = require('level')
var Sublevel = require('level-sublevel')

var userHome = (process.platform === 'win32' ? process.env.USERPROFILE : process.env.HOME)
var dbPath = path.join(userHome, 'npm-leveldb')

var db = Sublevel(level(dbPath, {valueEncoding: 'json'}))

var sublevels = {
  package: db.sublevel('package'),
  misc: db.sublevel('misc'),
  downloads: db.sublevel('downloads')
}

module.exports = sublevels
