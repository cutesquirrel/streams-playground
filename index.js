require('colour');
var async = require('async');
var casual = require('casual');
var csv = require("fast-csv");
var mongoose = require('mongoose');
var through = require('through');


// ---------------------------------------------------
// Settings.
// ---------------------------------------------------
var records_count = 100;
var dbname = 'test';
var AWS_BUCKET_NAME = 'homair-assetregister';
var FILENAME = 'my_data2.csv';
// ---------------------------------------------------

mongoose.connect('mongodb://localhost/' + dbname);
var db = mongoose.connection;
db.on('error', function(err) {
  console.error('✘ CANNOT CONNECT TO MongoDB DATABASE !'.red, dbname.blue, err);
});
db.on('disconnected', function() {
  console.log('✘ DISCONNECTED from MongoDB DATABASE !'.red);
});
db.on('parseError', function(err) {
  console.log('✘ parseError... '.red, err);
});
db.on('open', function(err) {
  console.log('✔ CONNECTED TO MongoDB DATABASE !'.green);


  // Once the connection has been established, let's start our job...
  // Let's use the amazing async library!
  async.series({
      purge: purge,
      fillDB: fillDB,
      querying: function(callback) {


        //var writeStream = getFileWriteStream(FILENAME);
        var writeStream = getS3WriteStream(FILENAME);

        querying(writeStream, callback);
      }
    },
    function(err) {
      if (err) {
        console.error(err);
        return process.exit(1);
      }

      console.log('');
      console.log('👍👍👍  The end ! 👍👍👍');

      process.exit(0);
    });
});


// Mongoose model/schema
var City = mongoose.model('City', {
  name: String,
  zip: Number,
  country: String,
});

function purge(callback) {
  dropCollection('cities', callback);
}

// ------------------------------------------------
// Preparing fake data...
// We could insert data in a bulk operation,
// but it's not the purpose here...
// ------------------------------------------------
function fillDB(callback) {

  console.log('★ Filling DB...'.yellow);

  var count = 0;

  // Well, a bit more async...
  // https://github.com/caolan/async#whilsttest-fn-callback
  async.whilst(
    function() {
      return count < records_count;
    },
    function(cb) {
      count++;


      var city = new City({
        name: casual.city,
        zip: casual.zip(5),
        country: casual.country
      });
      city.save(function(err) {
        if (err) {
          return cb(err);
        }

        if (count % 100 === 0 || count === records_count) {
          console.log('Inserting', count + '/' + records_count);
        }

        cb();
      });
    },
    callback
  );
}

// ------------------------------------------------
// Querying data...
// ------------------------------------------------
function querying(outputStream, callback) {
  console.log('★ Querying DB...'.yellow);


  // ==================
  // Create a Duplex Stream receiving a mongoDB document.
  // ==================
  var transform = through(function(doc) {
    // do something with the mongoose document
    doc = doc.toJSON ? doc.toJSON() : {};

    //... Here you can add some treamtments...

    // Pass the transformed doc to the next pipe.
    this.queue(doc);
  });
  // Error handling for this stream.
  transform.on('error', function(err) {
    callback(err);
  });


  // ==================
  // Creates a transform/duplex stream.
  // ==================
  var csvTransformStream = csv.createWriteStream({
    headers: true
  });
  // Error handling for this stream.
  csvTransformStream.on('error', function(err) {
    callback(err);
  });


  // ==================
  // Query stream!
  // ==================
  var line = 1;
  var stream = City.find().stream();

  stream.on('end', function() {
    console.log('End of reading data, nb of lines:'.green, line);
  });
  stream.on('data', function() {
    line++;
  });
  // Error handling for this stream.
  stream.on('error', function(err) {
    console.log('ERROR on query stream, line:'.red, line, ', ERROR:', err);
    callback(err);
  });


  // ==================
  // Our last stream, where we will write the final data.
  // ==================
  outputStream
  // Error handling for this stream.
    .on('error', function(err) {
      console.log('ERROR on output stream'.red, err);
      callback(err);
    })
    // It's only when all is writen than we can finish our process.
    .on('finish', function() {
      console.log('End of writing to output.'.green);
      callback();
    });


  // ==================
  // *** OUR PIPELINE ***
  // ==================
  stream
    .pipe(transform)
    .pipe(csvTransformStream)
    .pipe(outputStream);
}

//
// Create an upload stream writer to AWS S3.
//
function getS3WriteStream(filename) {
  var AWS = require('aws-sdk');
  var UploadStream = require("s3-stream-upload");

  var s3Obj = new AWS.S3();
  console.log('Found credentials:', s3Obj.config.credentials);


  var outputStream = new UploadStream(s3Obj, {
    Bucket: AWS_BUCKET_NAME,
    Key: filename
  });

  return outputStream;
}

//
// Create a file stream writer.
//
function getFileWriteStream(filename) {
  var fs = require('fs');
  var homedir = process.env[(process.platform === 'win32') ? 'USERPROFILE' : 'HOME'];
  var outputStream = fs.createWriteStream(homedir + '/Downloads/' + filename);


  return outputStream;
}

//
// Utility method to drop a mongodb collection.
//
function dropCollection(collectionName, done) {
  var collection = mongoose.connection.collections[collectionName];
  if (collection) {
    collection.drop(function(err) {
      //console.log(collectionName, 'dropped'.red);
      if (err && err.message !== 'ns not found') {
        return done(err);
      }

      console.log('Collection dropped with success!');

      done(null);
    });
  } else {
    // si collection non trouvée
    done('This collection does not exist.');
  }
}