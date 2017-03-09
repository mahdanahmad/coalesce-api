require('dotenv').config();
const async         = require('async');
const _             = require('lodash');
const moment        = require('moment');
const fs            = require('fs');

const ObjectID      = require('mongodb').ObjectID;
const MongoClient   = require('mongodb').MongoClient;

const auth          = (process.env.DB_USERNAME !== '' || process.env.DB_PASSWORD !== '') ? process.env.DB_USERNAME + ':' + process.env.DB_PASSWORD + '@' : '';
const db_url        = 'mongodb://' + auth + process.env.DB_HOST + ':' + process.env.DB_PORT +  '/' + process.env.DB_DATABASE;
MongoClient.connect(db_url, (err, db) => {
    if (err) { throw err; }

    let stacked		= db.collection('stacked');
    let swimlane	= db.collection('swimlane');

	async.waterfall([
		function(callback) {
			stacked.ensureIndex('frequency', { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			stacked.ensureIndex({date: 1, frequency: 1, tags: 1}, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			swimlane.ensureIndex('frequency', { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			swimlane.ensureIndex({startDate: 1, endDate: 1, frequency: 1}, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
	], (err, asyncResult) => {
		if (err) { throw err; }
		db.close();
	});
});
