const db        = require('../connection');

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
            db.getCollection('data').aggregate([{$group : { _id : 0, frequency : { $addToSet : '$frequency'} }} ], (err, res) => {
                if (err) { return flowCallback(err); }
                flowCallback(null, { frequency : _.chain(res).flatMap('frequency').sortBy().value() });
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
            db.getCollection('data').aggregate([
                { $match : { startDate : { $lte : endDate }, endDate : { $gte : startDate }, frequency : { $in : frequencies }}},
                { $project : {
                    startDate   : { $dateToString: { format: "%Y-%m-%d", date: { $add : [{ $cond: [{ $lte : [ '$startDate', startDate ] }, startDate, '$startDate' ]}, 7 * 60 * 60 * 1000]}}},
                    endDate     : { $dateToString: { format: "%Y-%m-%d", date: { $add : [{ $cond: [{ $gte : [ '$endDate', endDate ] }, endDate, '$endDate' ]}, 7 * 60 * 60 * 1000]}}},
                    count       : { $cond: [{ $eq : [datatype, 'rows'] }, '$rowcount', { $cond: [{ $eq : [datatype, 'filesize'] }, { $ceil : { $divide : ['$filesize', 1000] }}, { $literal : 1 }]}]},
                    tags : 1, datasets : 1
                }},
                { $group : { _id : '$datasets', tags : { $first : '$tags' }, count : { $sum : '$count' }, data : { $push : { s : '$startDate', e : '$endDate' } }}}
            ], (err, res) => {
                flowCallback(null, res);
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

    let startDate       = !_.isNil(input.startDate)     ? moment(input.startDate, "YYYY-MM-DD").toDate()    : moment().subtract(5, 'y').toDate();
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
            db.getCollection('data').aggregate([
                { $match : { startDate : { $lte : endDate }, endDate : { $gte : startDate }, frequency : { $in : frequencies }, tags : { $in : tags }}},
                { $project : {
                    s   : { $dateToString: { format: "%Y-%m-%d", date: { $add : [{ $cond: [{ $lte : [ '$startDate', startDate ] }, startDate, '$startDate' ]}, 7 * 60 * 60 * 1000]}}},
                    e   : { $dateToString: { format: "%Y-%m-%d", date: { $add : [{ $cond: [{ $gte : [ '$endDate', endDate ] }, endDate, '$endDate' ]}, 7 * 60 * 60 * 1000]}}},
                    c   : { $cond: [{ $eq : [datatype, 'rows'] }, '$rowcount', { $cond: [{ $eq : [datatype, 'filesize'] }, { $ceil : { $divide : ['$filesize', 1000] }}, { $literal : 1 }]}]},
                    f   : '$frequency'
                }},
            ], (err, res) => {
                async.map(res, (o, callback) => {
                    async.times(moment(o.e).diff(o.s, 'days') + 1, (d, next) => {
                        let currentDate = moment(o.s).add(d, 'd');
                        if (currentDate.isSameOrAfter(startDate) && currentDate.isSameOrBefore(endDate))  {
                            next(null, { date : currentDate.format('YYYY-MM-DD'), freq : o.f, val : o.c })
                        } else {
                            next(null, null);
                        }
                    }, function(err, results) {
                        callback(null, results);
                    });
                }, (err, results) => {
                    let chained = _.chain(results).flatten().compact().groupBy('date');
                    timeline    = chained.map((val, key) => ({date : key, data : _.chain(val).groupBy('freq').map((fval, fkey) => ({freq : parseInt(fkey), val : _.sumBy(fval, 'val')})).value()})).value();
                    maxData     = chained.map((o) => (_.sumBy(o, 'val'))).max().value();
                    if (maxData == 0) { maxData++; }

                    listDate    = chained.map((o, key) => (key));

                    flowCallback(null, { timeline, maxData, startDate : listDate.minBy((o) => (new Date(o))).value(), endDate : listDate.maxBy((o) => (new Date(o))).value() });
                });
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
