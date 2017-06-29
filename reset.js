var db = require('./lib/db.js')
db.downloads.del('sqlpad', function (err) {
  if (err) console.error(err)
  console.log('deleted')
})
