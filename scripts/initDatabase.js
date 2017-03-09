require('dotenv').config();
const async         = require('async');
const _             = require('lodash');
const moment        = require('moment');
const fs            = require('fs');

const ObjectID      = require('mongodb').ObjectID;
const MongoClient   = require('mongodb').MongoClient;

const filepath      = './public/result.json';

const auth          = (process.env.DB_USERNAME !== '' || process.env.DB_PASSWORD !== '') ? process.env.DB_USERNAME + ':' + process.env.DB_PASSWORD + '@' : '';
const db_url        = 'mongodb://' + auth + process.env.DB_HOST + ':' + process.env.DB_PORT +  '/' + process.env.DB_DATABASE;
MongoClient.connect(db_url, (err, db) => {
    if (err) { throw err; }

    let raw			= db.collection('raw');
    let stacked		= db.collection('stacked');
    let swimlane	= db.collection('swimlane');

	async.waterfall([
		function(callback) {
			raw.drop((err, result) => {
				// if (err) { return callback(err); }
				stacked.drop((err, result) => {
					// if (err) { return callback(err); }
					swimlane.drop((err, result) => {
						// if (err) { return callback(err); }
						callback();
					});
				});
			});
		},
		function(callback) {
			fs.readFile(filepath, 'utf8', (err, data) => {
				if (err) { return callback(err); }
				callback(null, _.map(JSON.parse(data), (o) => (_.assign({}, o, { freqs: _.chain(o.timeline).flatMap('frequency').uniq().value() }))));
			});
		},
		function(data, callback) {
			raw.insertMany(data, (err, res) => { if (err) { return callback(err); } callback(null, data); });
		},
		function(data, callback) {
			let coalesce    = [];
			async.each(data, function(o, datasetsCallback) {
				async.each(o.timeline, function(d, dataCallback) {
					coalesce.push({
						startDate	: moment(d.startDate).toDate(),
						endDate		: moment(d.endDate).toDate(),
						frequency	: d.frequency,
						rowcount	: d.rowscount,
						filesize	: d.filesize,
						tags		: _.map(o.tags, _.trim),
						group		: o.group,
						dataset		: o.dataset,
					});

					process.nextTick(function() { dataCallback(); });
				}, function(err) {
					if (err) { return callback(err); }
					process.nextTick(function() { datasetsCallback(); });
				});
			}, function(err) {
				if (err) { return callback(err); }
				swimlane.insertMany(coalesce, (err, res) => { if (err) { return callback(err); } process.nextTick(function() { callback(null, data); }); });
			});
		},
		function(data, callback) {
			let total	= _.chain(data).flatMap('timeline').map((o) => (moment(o.endDate).diff(o.startDate, 'days') + 1)).sum().value();
			let counter	= 0;
			let percent	= 0;
			async.eachSeries(data, (o, datasetsCallback) => {
				async.eachSeries(o.timeline, (t, dataCallback) => {
					async.timesSeries(moment(t.endDate).diff(t.startDate, 'days') + 1, (d, next) => {
				        next(null, {
		                    date		: moment(t.startDate).add(d, 'd').toDate(),
		                    frequency   : t.frequency,
		                    rowcount    : t.rowscount,
		                    filesize    : t.filesize,
		                    tags        : _.map(o.tags, _.trim),
		                    group       : o.groups,
		                    dataset		: o.dataset,
		                });
					}, function(err, results) {
						if (err) { return callback(err); }
						stacked.insertMany(results, (err, res) => {
							if (err) { return callback(err); }

							counter	+= results.length;
							temp	= Math.floor(counter * 100 / total);
							if (temp !== percent) { console.log(temp + '%'); percent = temp; }

							process.nextTick(function() { dataCallback(null); });
						});
					});
				}, function(err) {
					if (err) { return callback(err); }
					process.nextTick(function() { datasetsCallback(null); });
				});
			}, function(err) {
				if (err) { return callback(err); }
				process.nextTick(function() { callback(null); });
			});
		},
		// function(callback) {
		// 	stacked.ensureIndex('frequency', { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		// },
		function(callback) {
			console.log('Indexing database...');
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
