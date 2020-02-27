const file = require('pull-file');
const utf8 = require('pull-utf8-decoder');
const split = require('pull-split');
const pull = require('pull-stream');
const path = require('path');
const _ = require('lodash');
const async = require('async');
const turf = require('@turf/turf');
const fs = require('fs');
const simplify = require('simplify-geojson');

const fileName = '510858.ndjson';

const SOURCE_FILE = path.resolve(`raw/${fileName}`);

const writeResult = pull.asyncMap((doc, cb) => {
  const stringifyDoc = JSON.stringify(doc);
  fs.writeFile(
    `result/simplify_510858.ndjson`,
    `${stringifyDoc}`,
    'utf8',
    () => {
      cb(null, doc);
    }
  );
});

/**
 * A source pull-stream that yields lines from the source file
 */
const sourceReadFileLines = sourceFile =>
  pull(file(sourceFile), utf8(), split());

/**
 * At the previous step, if the line was invalid, a null would be sent through...
 */
const throughFilterOutEmptyLines = pull.filter(data => !_.isEmpty(data));

const asyncJsonParse = async.asyncify(JSON.parse);
/**
 * Get a template document from the JSON line.
 */
const throughParseLineToTemplate = pull.asyncMap(asyncJsonParse);

/**
 * Removes redundant coordinates from any GeoJSON Geometry.
 */
function cleanFeatureCoords(feature, cb) {
  if (
    feature.geometry.type === 'Point' ||
    feature.geometry.type === 'MultiPoint'
  ) {
    return cb(null, feature);
  }

  // Do not try to clean empty arrays
  if (!feature.geometry.coordinates) {
    return cb(null, feature);
  }

  cb(null, turf.cleanCoords(feature));
}

const cleanFeatureCoordsTransform = pull.asyncMap((feat, cb) =>
  cleanFeatureCoords(feat, cb)
);

const reduceDecimal = pull.asyncMap((feat, cb) => {
  const options = {precision: 6, coordinates: 2};
  const truncated = turf.truncate(feat, options);
  cb(null, truncated);
});

/**
 * see https://github.com/maxogden/simplify-geojson
 */
const simplifyFeature = pull.asyncMap((feat, cb) => {
  const tolerance = 0.0001; // increase number to simplify more
  const simplifiedFeature = simplify(feat, tolerance);
  cb(null, simplifiedFeature);
});

pull(
  sourceReadFileLines(SOURCE_FILE),
  throughFilterOutEmptyLines,
  throughParseLineToTemplate,
  simplifyFeature,
  reduceDecimal,
  cleanFeatureCoordsTransform,
  writeResult,
  pull.drain(null, error => {
    if (error) {
      console.log('pull-stream:', error);
    }
    console.log('convert finished');
  })
);
