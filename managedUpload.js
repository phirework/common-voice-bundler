const config = require('./config');
const fs = require('fs');
const path = require('path');
const tar = require('tar');
const { PassThrough } = require('stream');
const {
  logProgress
} = require('./helpers');


const { name: outBucketName } = config.get(
  'outBucket'
);

const OUT_DIR = config.get('localOutDir');


const uploadDataset = (localeDirs, bundlerBucket, releaseName) => {
  return localeDirs.reduce((promise, locale) => {
    return promise.then(sizes => {
      const stream = new PassThrough();
      const archiveName = `${releaseName}/${locale}.tar.gz`;
      console.log('archiving & uploading', archiveName);
      const managedUpload = bundlerBucket.upload({
        Body: stream,
        Bucket: outBucketName,
        Key: archiveName,
        ACL: 'public-read'
      });
      logProgress(managedUpload);

      const localeDir = path.join(OUT_DIR, locale);
      tar
        .c({ gzip: true, cwd: localeDir }, fs.readdirSync(localeDir))
        .pipe(stream);

      return managedUpload
        .promise()
        .then(() =>
          bundlerBucket
            .headObject({ Bucket: outBucketName, Key: archiveName })
            .promise()
        )
        .then(({ ContentLength }) => {
          console.log('');
          sizes[locale] = { size: ContentLength };
          return sizes;
        })
        .catch(err => console.error(err));
    });
  }, Promise.resolve({}));
}

module.exports = {
  uploadDataset
}