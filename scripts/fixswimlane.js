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

    let swimlane	= db.collection('swimlane');

	swimlane.find({}).toArray((err, result) => {
		async.each(result, (o, next) => {
			console.log(o.tag);
			swimlane.findOneAndUpdate({ _id: o._id }, {
				$set: {
					frequency: parseInt(o.frequency),
					datelist: _.map(o.datelist, (d) => (moment(d).toDate()))
				}
			}, {upsert: true}, (err, res) => {
				if (err) { return next(err); };
				next(null);
			});
		}, (err) => {
			if (err) throw err;
			db.close();
		});
	});
});
