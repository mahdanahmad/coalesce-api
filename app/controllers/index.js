const db        = require('../connection');
const round     = require('mongo-round');

const async     = require('async');
const _         = require('lodash');
const moment    = require('moment');

module.exports.config   = (callback) => {
    let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get config data success.';
    let result          = null;
    let missingParams   = [];

    async.waterfall([
        function (flowCallback) {
			db.getCollection('swimlane').distinct('frequency', (err, result) => {
				flowCallback(null, { frequency : _.sortBy(result) });
			});
        },
    ], (err, asyncResult) => {
        if (err) {
            response    = 'FAILED';
            status_code = 400;
            message     = err;
        } else {
            result      = asyncResult;
        }
        callback({ response, status_code, message, result });
    });
};

module.exports.selector  = (input, callback) => {
    let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get selector data success.';
    let result          = null;
    let missingParams   = [];

    let startDate       = !_.isNil(input.startDate)     ? moment(input.startDate, "YYYY-MM-DD").toDate()    : moment().subtract(5, 'y').toDate();
    let endDate         = !_.isNil(input.endDate)       ? moment(input.endDate, "YYYY-MM-DD").toDate()      : moment().toDate();
    let datatype        = !_.isNil(input.datatype)      ? input.datatype                                    : '';
    let frequencies     = !_.isNil(input.frequencies)   ? JSON.parse(input.frequencies)                     : [];

    async.waterfall([
		function (flowCallback) {
			db.getCollection('raw').aggregate([
				{ $match: { 'freqs': { '$in': frequencies }}},
				{ $unwind: '$tags' },
				{ $group: { '_id': '$tags', 'count': { '$sum': 1 }}},
				{ $sort: { 'count': -1 }},
				{ $limit: 20 }
			], (err, res) => {
				if (err) { return flowCallback(err); } else { return flowCallback(null, _.map(res, '_id')); }
			});
		},
        function (top20tags, flowCallback) {
            db.getCollection('swimlane').aggregate([
                { $match: { startDate : { $lte : endDate }, endDate : { $gte : startDate }, frequency : { $in : frequencies }, tags: { $in: top20tags }}},
                { $project: {
                    startDate	: { $dateToString: { format: "%Y-%m-%d", date: { $add: [{ $cond: [{ $lte: [ '$startDate', startDate ] }, startDate, '$startDate' ]}, 7 * 60 * 60 * 1000]}}},
                    endDate		: { $dateToString: { format: "%Y-%m-%d", date: { $add: [{ $cond: [{ $gte: [ '$endDate', endDate ] }, endDate, '$endDate' ]}, 7 * 60 * 60 * 1000]}}},
                    count		: { $cond: [{ $eq: [datatype, 'rows'] }, '$rowcount', { $cond: [{ $eq : [datatype, 'filesize'] }, round({ $divide: ['$filesize', 1000] }), { $literal: 1 }]}]},
                    tags		: { $filter: { input: '$tags', as: 'tag', cond: { $in: ['$$tag', top20tags]}}},
					dataset		: 1
                }},
                { $group: { _id: '$dataset', tags: { $first: '$tags' }, count: { $sum: '$count' }, data: { $push: { s: '$startDate', e: '$endDate' } }}}
            ], { explain: false }, (err, res) => {
                if (err) { return flowCallback(err); } else { return flowCallback(null, res); }
            });
        },
    ], (err, asyncResult) => {
        if (err) {
            response    = 'FAILED';
            status_code = 400;
            message     = err;
        } else {
            result      = asyncResult;
        }
        callback({ response, status_code, message, result });
    });
};

module.exports.stacked  = (input, callback) => {
    let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get selector data success.';
    let result          = null;
    let missingParams   = [];

    let startDate       = !_.isNil(input.startDate)     ? moment(input.startDate, "YYYY-MM-DD").toDate()    : moment().subtract(6, 'y').toDate();
    let endDate         = !_.isNil(input.endDate)       ? moment(input.endDate, "YYYY-MM-DD").toDate()      : moment().toDate();
    let datatype        = !_.isNil(input.datatype)      ? input.datatype                                    : '';
    let tags            = !_.isNil(input.tags)          ? JSON.parse(input.tags)                            : null;
    let frequencies     = !_.isNil(input.frequencies)   ? JSON.parse(input.frequencies)                     : [];

    if (_.isNil(input.tags)) { missingParams.push('tags'); }

    async.waterfall([
        function (flowCallback) {
			if (!_.isEmpty(missingParams)) {
				flowCallback('Missing parameters : {' + missingParams.join(', ') + '}');
			} else {
				flowCallback(null);
			}
		},
        function (flowCallback) {
            db.getCollection('stacked').aggregate([
                { $match: { date: { $lte: endDate, $gte: startDate }, frequency: { $in: frequencies }, tags: { $in: tags }}},
                { $project : {
                    d: { $dateToString: { format: "%Y-%m-%d", date: { $add : ['$date', 7 * 60 * 60 * 1000]}}},
                    c: { $cond: [{ $eq : [datatype, 'rows'] }, '$rowcount', { $cond: [{ $eq : [datatype, 'filesize'] }, round({ $divide : ['$filesize', 1000] }), { $literal : 1 }]}]},
                    f: '$frequency'
                }},
				{ $group : { _id : { d: '$d', f: '$f' }, c: { $sum: '$c' }}},
				{ $project : { _id: '$_id.d', f: '$_id.f', c: '$c' }},
				{ $group : { _id: '$_id', data: { $push: { f: '$f', c: '$c' }}}},
				{ $sort : { _id : 1 }},
            ], { explain: false }, (err, res) => {
				if (err) { return flowCallback(err); } else {
					let max
					return flowCallback(null, { timeline: res, startDate: _.chain(res).first().get('_id', moment().format('YYYY-MM-DD')), endDate: _.chain(res).last().get('_id', moment().format('YYYY-MM-DD')), maxData: _.chain(res).map((o) => _.sumBy(o.data, 'c')).max().value() });
				}
            });
        },
    ], (err, asyncResult) => {
        if (err) {
            response    = 'FAILED';
            status_code = 400;
            message     = err;
        } else {
            result      = asyncResult;
        }
        callback({ response, status_code, message, result });
    });
};

module.exports.datasets	= (input, callback) => {
    let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get datasets data success.';
    let result          = null;
    let missingParams   = [];

    let tags            = !_.isNil(input.tags)          ? JSON.parse(input.tags)		: null;
    let frequencies     = !_.isNil(input.frequencies)   ? JSON.parse(input.frequencies)	: [];

    if (_.isNil(input.tags)) { missingParams.push('tags'); }
    if (_.isNil(input.frequencies)) { missingParams.push('frequencies'); }

    async.waterfall([
        function (flowCallback) {
			if (!_.isEmpty(missingParams)) {
				flowCallback('Missing parameters : {' + missingParams.join(', ') + '}');
			} else {
				flowCallback(null);
			}
		},
        function (flowCallback) {
            db.getCollection('raw').aggregate([
                { $match: { freqs: { $in: frequencies }, tags: { $in: tags }}},
                { $project : {
                    name: '$dataset',
					tags: { $filter: { input: '$tags', as: 'tag', cond: { $in: ['$$tag', tags]}}},
					frequency: { $filter: { input: '$freqs', as: 'freq', cond: { $in: ['$$freq', frequencies]}}},
                }},
            ], { explain: false }, (err, res) => {
				if (err) { return flowCallback(err); } else {
					let max
					return flowCallback(null, res);
				}
            });
        },
    ], (err, asyncResult) => {
        if (err) {
            response    = 'FAILED';
            status_code = 400;
            message     = err;
        } else {
            result      = asyncResult;
        }
        callback({ response, status_code, message, result });
    });
};
