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
	let force		= db.collection('force');
    let stacked		= db.collection('stacked');
    let swimlane	= db.collection('swimlane');

	async.waterfall([
		function(callback) {
			fs.readFile(filepath, 'utf8', (err, data) => {
				if (err) { return callback(err); }
				callback(null, JSON.parse(data));
			});
		},
		function(data, callback) {
			let unwinded	= _.chain(data)
								.flatMap((o) => { let timeline = _.map(o.timeline, (t) => (_.pick(t, ['startDate', 'endDate', 'frequency']))); return _.map(o.tags, (tag) => ({ tag, timeline })); })
								.groupBy('tag')
								.map((o, key) => ({tag: _.trim(key), timeline: _.flatMap(o, 'timeline')}))
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
						let datelist	= _.chain(range).flatMap((m) => (_.times(moment(m.endDate).diff(m.startDate, 'days') + 1, (x) => (moment(m.startDate).add(x, 'd').format('YYYY-MM-DD'))))).uniq().value();
						return ({ frequency, range, datelist, tag: o.tag });
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
		}
	], (err, asyncResult) => {
		if (err) { throw err; }
		db.close();
	});
});
