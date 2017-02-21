require('dotenv').config();
const async         = require('async');
const _             = require('lodash');
const moment        = require('moment');
const fs            = require('fs');

const ObjectID      = require('mongodb').ObjectID;
const MongoClient   = require('mongodb').MongoClient;

const filepath      = './public/elnino-result.json';

const auth          = (process.env.DB_USERNAME !== '' || process.env.DB_PASSWORD !== '') ? process.env.DB_USERNAME + ':' + process.env.DB_PASSWORD + '@' : '';
const db_url        = 'mongodb://' + auth + process.env.DB_HOST + ':' + process.env.DB_PORT +  '/' + process.env.DB_DATABASE;
MongoClient.connect(db_url, function(err, db) {
    if (err) { throw err; }

    let collection  = db.collection('data');
    collection.drop();

    fs.readFile(filepath, 'utf8', function (err, data) {
        if (err) throw err;
        let coalesce    = [];
        async.each(JSON.parse(data), function(o, datasetsCallback) {
            async.each(o.d, function(d, dataCallback) {
                coalesce.push({
                    startDate   : moment(d.s).toDate(),
                    endDate     : moment(d.e).toDate(),
                    frequency   : d.f,
                    rowcount    : d.r,
                    filesize    : d.z,
                    tags        : o.t,
                    group       : o.g,
                    datasets    : o.n,
                });
                dataCallback();
            }, function(err) {
                if (err) { throw err; }
                datasetsCallback();
            });
        }, function(err) {
            if (err) { throw err; }
            collection.insertMany(coalesce, (err, res) => { if (err) { throw err; } db.close(); });
        });
    });

});
