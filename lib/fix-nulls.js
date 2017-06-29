var _ = require('lodash')

/*  fixNulls
    there might be \u0000 characters in readmes or descriptions (or anywhere really)
    these are unicode NULL characters and postgres does not like these.
    they must be removed.
    this function recursively traverses the object and cleans up any nulls it finds.
============================================================================= */
function fixNulls (obj) {
  _.forOwn(obj, function (value, key) {
    if (_.isString(value)) {
      obj[key] = value.replace(/\u0000/g, '')
    } else if (_.isObject(value)) {
      fixNulls(value)
    }
  })
}

module.exports = fixNulls
