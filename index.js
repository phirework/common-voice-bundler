require('dotenv').config();

const fs = require('fs');
const path = require('path');
const merge = require('lodash.merge');
const config = require('./config');

const {
  countFileLines,
  promptLoop,
  unitToHours,
  getLocaleDirs,
  sumDurations
} = require('./helpers');

const OUT_DIR = config.get('localOutDir');
const RELEASE_NAME = config.get('releaseName');
const TSV_PATH = path.join(OUT_DIR, 'clips.tsv');
const { name: OUT_BUCKET_NAME } = config.get('outBucket');

let localeDirs = [];

const { db, clipBucket, bundlerBucket } = require('./init').initialize();

const { processAndDownloadClips } = require('./getClips');
const { uploadDataset } = require('./getClips');


const _countBuckets = async () => {
  const query = `In a separate shell, run the following command:
    create-corpora -f ${TSV_PATH} -d ${OUT_DIR} -v\n
When that has completed, return to this shell and type 'corpora-complete' and hit enter > `

  await promptLoop(query, {
    'corpora-complete': () => { return; }
  });

  const buckets = {};
  for (const locale of localeDirs) {
    const localePath = path.join(OUT_DIR, locale);
    const localeBuckets = (await fs.readdirSync(localePath))
      .filter(file => file.endsWith('.tsv'))
      .map(async fileName => [
        fileName,
        Math.max((await countFileLines(path.join(localePath, fileName))) - 1, 0)
      ]);
    buckets[locale] = {
      buckets: (await Promise.all(localeBuckets)).reduce(
        (obj, [key, count]) => {
          obj[key.split('.tsv')[0]] = count;
          return obj;
        },
        {}
      )
    };
  }
  return buckets;
};

// const _archiveAndUpload = () =>  uploadDataset(RELEASE_NAME, bundlerBucket)

  // localeDirs.reduce((promise, locale) => {
  //   return promise.then(sizes => {
  //     const stream = new PassThrough();
  //     const archiveName = `${RELEASE_NAME}/${locale}.tar.gz`;
  //     console.log('archiving & uploading', archiveName);
  //     const managedUpload = outBucket.upload({
  //       Body: stream,
  //       Bucket: OUT_BUCKET_NAME,
  //       Key: archiveName,
  //       ACL: 'public-read'
  //     });
  //     logProgress(managedUpload);

  //     const localeDir = path.join(OUT_DIR, locale);
  //     tar
  //       .c({ gzip: true, cwd: localeDir }, fs.readdirSync(localeDir))
  //       .pipe(stream);

  //     return managedUpload
  //       .promise()
  //       .then(() =>
  //         outBucket
  //           .headObject({ Bucket: OUT_BUCKET_NAME, Key: archiveName })
  //           .promise()
  //       )
  //       .then(({ ContentLength }) => {
  //         console.log('');
  //         sizes[locale] = { size: ContentLength };
  //         return sizes;
  //       })
  //       .catch(err => console.error(err));
  //   });
  // }, Promise.resolve({}));

const calculateAggregateStats = stats => {
  let totalDuration = 0;
  let totalValidDurationSecs = 0;

  for (const locale in stats.locales) {
    const lang = stats.locales[locale];
    const validClips = lang.buckets ? lang.buckets.validated : 0;

    lang.avgDurationSecs = Math.round((lang.duration / lang.clips)) / 1000;
    lang.validDurationSecs = Math.round((lang.duration / lang.clips) * validClips) / 1000;

    lang.totalHrs = unitToHours(lang.duration, 'ms', 2);
    lang.validHrs = unitToHours(lang.validDurationSecs, 's', 2);

    stats.locales[locale] = lang;

    totalDuration += lang.duration;
    totalValidDurationSecs += lang.validDurationSecs;
  }

  stats.totalDuration = Math.floor(totalDuration);
  stats.totalValidDurationSecs = Math.floor(totalValidDurationSecs);
  stats.totalHrs = unitToHours(stats.totalDuration, 'ms', 0);
  stats.totalValidHrs = unitToHours(stats.totalValidDurationSecs, 's', 0);

  return stats;
}

const collectAndUploadStats = async stats => {
  const statsJSON = calculateAggregateStats({
    bundleURLTemplate: `https://${OUT_BUCKET_NAME}.s3.amazonaws.com/${RELEASE_NAME}/{locale}.tar.gz`,
    locales: merge(...stats)
  });

  console.dir(statsJSON, { depth: null, colors: true });
  return bundlerBucket
    .putObject({
      Body: JSON.stringify(statsJSON),
      Bucket: OUT_BUCKET_NAME,
      Key: `${RELEASE_NAME}/stats.json`,
      ACL: 'public-read'
    })
    .promise();
};

const archiveAndUpload = async () => {
  return config.get('skipBundling') ? Promise.resolve() : uploadDataset(localeDirs, bundlerBucket, RELEASE_NAME);
}

const countBuckets = async () => {
  return config.get('skipCorpora') ? Promise.resolve() : _countBuckets();
}

const run = () => {
  processAndDownloadClips(db, clipBucket)
    .then(stats => {
      localeDirs = getLocaleDirs(OUT_DIR);

      return Promise.all([
        stats,
        sumDurations(),
        countBuckets().then(async bucketStats =>
          merge(
            bucketStats,
            await archiveAndUpload()
          )
        )
      ]);
    })
    .then(collectAndUploadStats)
    .catch(e => console.error(e))
    .finally(() => process.exit(0));
}

run();