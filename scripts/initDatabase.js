require('dotenv').config();
const util 			= require('util');

const async         = require('async');
const _             = require('lodash');
const moment        = require('moment');
const fs            = require('fs');

const ObjectID      = require('mongodb').ObjectID;
const MongoClient   = require('mongodb').MongoClient;

const files			= ['data.go.id', 'data.humdata.org'];

const stoptags		= './public/data.go.id-remove.csv';
const wilayahfile	= './public/mappedWilayah.json';

const auth          = (process.env.DB_USERNAME !== '' || process.env.DB_PASSWORD !== '') ? process.env.DB_USERNAME + ':' + process.env.DB_PASSWORD + '@' : '';
const db_url        = 'mongodb://' + auth + process.env.DB_HOST + ':' + process.env.DB_PORT +  '/' + process.env.DB_DATABASE;
MongoClient.connect(db_url, (err, db) => {
    if (err) { throw err; }

    let raw			= db.collection('raw');
	let force		= db.collection('force');
    let stacked		= db.collection('stacked');
    let swimlane	= db.collection('swimlane');

	async.waterfall([
		function(callback) {
			raw.drop((err, result) => {
				// if (err) { return callback(err); }
				stacked.drop((err, result) => {
					// if (err) { return callback(err); }
					force.drop((err, result) => {
						// if (err) { return callback(err); }
						swimlane.drop((err, result) => {
							// if (err) { return callback(err); }
							callback();
						});
					});
				});
			});
		},
		function(callback) {
			fs.readFile(stoptags, 'utf8', (err, data) => {
				if (err) { return callback(err); }

				let removedtags	= data.split(/\r?\n/);
				fs.readFile(wilayahfile, (err, data) => {
					if (err) { return callback(err); }

					callback(null, removedtags, JSON.parse(data));
				});
			});
		},
		function(removedtags, wilayahs, callback) {
			async.map(files, (val, next) => {
				fs.readFile('./public/' + val + '-result.json', 'utf8', (err, data) => {
					if (err) { return callback(err); }

					next(null, _.map(JSON.parse(data), (o) => (_.assign({}, o, { freqs: _.chain(o.timeline).flatMap('frequency').uniq().value(), source : val, wilayah: wilayahs[o.dataset], tags: _.difference(o.tags, removedtags) }))));
				});
			}, (err, results) => {
				if (err) { return callback(err); }

				// callback(null, _.flatten(results));
				callback(null, _.chain(results).flatten().sampleSize(100).value());
			})
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
						source		: o.source,
						wilayah		: o.wilayah,
					});

					process.nextTick(function() { dataCallback(); });
				}, function(err) {
					if (err) { return callback(err); }
					process.nextTick(function() { datasetsCallback(); });
				});
			}, function(err) {
				if (err) { return callback(err); }
				force.insertMany(coalesce, (err, res) => { if (err) { return callback(err); } process.nextTick(() => { callback(null, data); }); });
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
							source		: o.source,
							wilayah		: o.wilayah,
		                });
					}, function(err, results) {
						if (err) { return callback(err); }
						stacked.insertMany(results, (err, res) => {
							if (err) { return callback(err); }

							counter	+= results.length;
							temp	= Math.floor(counter * 100 / total);
							if (temp !== percent) { console.log(temp + '%'); percent = temp; }

							process.nextTick(() => { dataCallback(null); });
						});
					});
				}, function(err) {
					if (err) { return callback(err); }
					process.nextTick(() => { datasetsCallback(null); });
				});
			}, function(err) {
				if (err) { return callback(err); }
				process.nextTick(() => { callback(null, data); });
			});
		},
		function(data, callback) {
			let unwinded	= _.chain(data)
								.flatMap((o) => { let timeline = _.map(o.timeline, (t) => (_.pick(t, ['startDate', 'endDate', 'frequency']))); return _.map(o.tags, (tag) => ({ tag, timeline, source: o.source })); })
								.groupBy((o) => (_.trim(o.tag) + '~' + o.source))
								.map((o, key) => ({ tag: key.split('~')[0], timeline: _.flatMap(o, 'timeline'), source: key.split('~')[1] }))
								.sortBy('tag')
								.value();

			async.eachSeries(unwinded, (o, datasetsCallback) => {
				async.mapSeries(o.timeline, (t, dataCallback) => {
					async.timesSeries(moment(t.endDate).diff(t.startDate, 'days') + 1, (d, next) => {
				        next(null, moment(t.startDate).add(d, 'd').toDate());
					}, function(err, results) {
						if (err) { return callback(err); }
						process.nextTick(() => { dataCallback(null, { results, frequency: t.frequency }); });
					});
				}, function(err, results) {
					if (err) { return callback(err); }

					let uniqtimeline	= _.chain(results).flatMap().groupBy('frequency').mapValues((o) => (_.chain(o).flatMap('results').uniqBy((o) => (moment(o).format("YYYY-MM-DD"))).sortBy().value())).value();

					let	list			= {};
					_.forEach(uniqtimeline, (eachfreq, freq) => {
						list[freq]		= [];
						let startDate	= null;
						let prevDate	= null;
						_.forEach(eachfreq, (u) => {
							if (_.isNull(startDate)) {
								startDate	= u;
								prevDate	= u;
							} else {
								if (moment(u).diff(prevDate, 'days') == 1) {
									prevDate	= u;
								} else {
									list[freq].push({ startDate, endDate: prevDate });
									startDate	= u;
									prevDate	= u;
								}
							}

						});
						list[freq].push({ startDate, endDate: prevDate });
					});

					list = _.map(list, (range, frequency) => {
						let datelist	= _.chain(range).flatMap((m) => (_.times(moment(m.endDate).diff(m.startDate, 'days') + 1, (x) => (moment(m.startDate).add(x, 'd').format('YYYY-MM-DD'))))).uniq().map((x) => (moment(x).toDate())).value();
						return ({ frequency: parseInt(frequency), range, datelist, tag: o.tag, source: o.source });
					});

					console.log(o.tag);
					swimlane.insertMany(list, (err) => {
						if (err) { return datasetsCallback(err); }

						process.nextTick(() => { datasetsCallback(null); });
					});

				});
			}, function(err) {
				if (err) { return callback(err); }
				process.nextTick(() => { callback(null); });
			});
		},
		function(callback) {
			console.log('Indexing database...');
			raw.ensureIndex({ frequency: 1, source: 1 }, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			raw.ensureIndex({ wilayah:1, source: 1 }, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			raw.ensureIndex({ frequency: 1, tags:1, source: 1 }, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			force.ensureIndex({frequency: 1, source: 1}, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			force.ensureIndex({startDate: 1, endDate: 1, frequency: 1, source: 1}, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			force.ensureIndex({startDate: 1, endDate: 1, frequency: 1, source: 1, wilayah: 1}, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			swimlane.ensureIndex({tag: 1, frequency: 1, source: 1}, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
		function(callback) {
			stacked.ensureIndex({date: 1, frequency: 1, tags: 1, source: 1}, { background: true }, (err) => { if (err) { return callback(err); } else { return callback(null); } });
		},
	], (err, asyncResult) => {
		if (err) { throw err; }
		db.close();
	});
});
