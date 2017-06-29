var semver = require('semver')

module.exports = function latestVersion (versionsArray) {
  var sortedVersions = versionsArray.filter(function (v) {
    return semver.valid(v)
  }).sort(semver.compare)

  var latestStableVersion

  for (var i = 0; i < sortedVersions.length; i++) {
    var prerelease = semver.prerelease(sortedVersions[i], true)
    if (!prerelease) latestStableVersion = sortedVersions[i]
  }

  return latestStableVersion
}
