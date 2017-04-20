const db        = require('../connection');
const round     = require('mongo-round');

const async     = require('async');
const _         = require('lodash');
const moment    = require('moment');

module.exports.config   = (input, callback) => {
    let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get config data success.';
    let result          = null;
    let missingParams   = [];

	let source			= !_.isNil(input.source) ? JSON.parse(input.source)	: [];

    async.waterfall([
        function (flowCallback) {
			db.getCollection('force').distinct('frequency', { source : { $in: source }}, (err, frequencies) => {
				if (err) { return flowCallback(err); }

				db.getCollection('force').distinct('wilayah', { source : { $in: source }}, (err, locations) => {
					if (err) { return flowCallback(err); }

					flowCallback(null, { frequency : _.sortBy(frequencies), location: _.sortBy(locations) });
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

module.exports.selector  = (input, callback) => {
    let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get selector data success.';
    let result          = null;
    let missingParams   = [];

    let startDate       = !_.isNil(input.startDate)     ? moment(input.startDate, "YYYY-MM-DD").toDate()    : moment().subtract(5, 'y').toDate();
    let endDate         = !_.isNil(input.endDate)       ? moment(input.endDate, "YYYY-MM-DD").toDate()      : moment().toDate();
	let numtags			= !_.isNil(input.numtags)		? _.toInteger(input.numtags)						: 20;
    let datatype        = !_.isNil(input.datatype)      ? input.datatype                                    : '';
    let frequencies     = !_.isNil(input.frequencies)   ? JSON.parse(input.frequencies)                     : [];
	let source			= !_.isNil(input.source)   		? JSON.parse(input.source)							: [];
	let wilayah			= !_.isNil(input.location)		? {$in: [input.location]}							: undefined;

	let explain     	= !_.isNil(input.explain)   	? (input.explain == 'true')							: false;

    async.waterfall([
		function (flowCallback) {
			db.getCollection('raw').aggregate([
				{ $match: _.omitBy({ freqs: { $in: frequencies }, source : { $in: source }, wilayah}, _.isNil)},
				{ $unwind: '$tags' },
				{ $group: { '_id': '$tags', 'count': { '$sum': 1 }}},
				{ $sort: { 'count': -1 }},
				{ $limit: numtags }
			], (err, res) => {
				if (err) { return flowCallback(err); } else { return flowCallback(null, _.map(res, '_id')); }
			});
		},
        function (top20tags, flowCallback) {
            db.getCollection('force').aggregate([
                { $match: { startDate : { $lte : endDate }, endDate : { $gte : startDate }, frequency : { $in : frequencies }, tags: { $in: top20tags }, source : { $in: source }}},
                { $project: {
                    count		: { $cond: [{ $eq: [datatype, 'rows'] }, '$rowcount', { $cond: [{ $eq : [datatype, 'filesize'] }, round({ $divide: ['$filesize', 1000000] }), { $literal: 1 }]}]},
                    tags		: { $filter: { input: '$tags', as: 'tag', cond: { $in: ['$$tag', top20tags]}}},
					dataset		: 1
                }},
                { $group: { _id: '$dataset', tags: { $first: '$tags' }, count: { $sum: '$count' }}},
				// { $unwind: '$tag' },
				// { $group: { _id: '$tag', count: { $sum: '$count' }}}
            ], { explain }, (err, res) => {
				if (err) { return flowCallback(err); }

				if (!explain) {
					let datasets    = _.chain(res);
					let nodeData    = datasets.flatMap((o) => (_.map(o.tags, (tag) => ( { tag : tag, count : o.count } ), []))).groupBy('tag').map((val, key) => ({name : key, count : _.sumBy(val, 'count')})).value();
					let linkData    = datasets.map('tags').flatMap((tags) => (_.reduce(tags, (result, value) => {
						let data    = [];
						if (_.size(result.tags) > 0) { _.forEach(result.tags, (o) => { data.push([o, value]); }); }
						return { data : _.concat((result['data'] || (result['data'] = [])), data), tags : _.concat(result['tags'] || (result['tags'] = []), value) };
					}, {}))['data']).groupBy((o) => (o)).map((val, key) => ({ source : key.split(',')[0], target : key.split(',')[1], count : _.size(val) })).value();

					return flowCallback(null, { nodeData, linkData });
				} else {
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

module.exports.swimlane	= (input, callback) => {
	let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get swimlane data success.';
    let result          = null;
    let missingParams   = [];

    let startDate       = !_.isNil(input.startDate)     ? moment(input.startDate, "YYYY-MM-DD").toDate()    : moment().subtract(5, 'y').toDate();
    let endDate         = !_.isNil(input.endDate)       ? moment(input.endDate, "YYYY-MM-DD").toDate()      : moment().toDate();
    let tags     		= !_.isNil(input.tags)   		? JSON.parse(input.tags)                     		: [];
    let frequencies     = !_.isNil(input.frequencies)   ? JSON.parse(input.frequencies)                     : [];
	let source			= !_.isNil(input.source)   		? JSON.parse(input.source)							: [];

	let explain     	= !_.isNil(input.explain)   	? (input.explain == 'true')							: false;

    async.waterfall([
        function (flowCallback) {
            db.getCollection('swimlane').aggregate([
                { $match: { tag : { $in : tags }, frequency : { $in : frequencies }, source : { $in: source }}},
				{ $project: {
					datelist: { $filter: { input: '$datelist', as: 'date', cond: { $and: [{ $gte: ['$$date', startDate]}, { $lte: ['$$date', endDate] }]}}},
					range: { $filter: { input: '$range', as: 'drop', cond: { $and: [{ $gte: ['$$drop.endDate', startDate]}, { $lte: ['$$drop.startDate', endDate]}]}}},
					tag: 1, frequency: 1,
				}},
				{ $group : { _id: '$tag', range: { $addToSet: '$range' }, datelist: { $addToSet: '$datelist' }}}
            ], { explain }, (err, res) => {
				if (err) { return flowCallback(err); }
				if (!explain) {
					return flowCallback(null, _.map(res, (o) => ({ tag: o._id, range: _.chain(o.range).flatMap().map((o) => ({ startDate: (moment(startDate).isAfter(o.startDate) ? input.startDate : moment(o.startDate).format('YYYY-MM-DD')), endDate: (moment(endDate).isBefore(o.endDate) ? input.endDate : moment(o.endDate).format('YYYY-MM-DD'))})).value(), count: _.chain(o.datelist).flatMap().uniqBy((d) => (moment(d).format('YYYY-MM-DD'))).size().value() })));
				} else {
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
	let source			= !_.isNil(input.source)   		? JSON.parse(input.source)							: [];

	let explain     	= !_.isNil(input.explain)   	? (input.explain == 'true')							: false;

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
                { $match: { date: { $lte: endDate, $gte: startDate }, frequency: { $in: frequencies }, tags: { $in: tags }, source : { $in: source }}},
                { $project : {
                    d: { $dateToString: { format: "%Y-%m-%d", date: { $add : ['$date', 7 * 60 * 60 * 1000]}}},
                    c: { $cond: [{ $eq : [datatype, 'rows'] }, '$rowcount', { $cond: [{ $eq : [datatype, 'filesize'] }, round({ $divide : ['$filesize', 1000000] }), { $literal : 1 }]}]},
                    f: '$frequency'
                }},
				{ $group : { _id : { d: '$d', f: '$f' }, c: { $sum: '$c' }}},
				{ $project : { _id: '$_id.d', f: '$_id.f', c: '$c' }},
				{ $group : { _id: '$_id', data: { $push: { f: '$f', c: '$c' }}}},
				{ $sort : { _id : 1 }},
            ], { explain }, (err, res) => {
				if (err) { return flowCallback(err); }
				if (!explain) {
					return flowCallback(null, { timeline: res, startDate: _.chain(res).first().get('_id', moment().format('YYYY-MM-DD')), endDate: _.chain(res).last().get('_id', moment().format('YYYY-MM-DD')), maxData: _.chain(res).map((o) => _.sumBy(o.data, 'c')).max().value() });
				} else {
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

module.exports.datasets	= (input, callback) => {
    let response        = 'OK';
    let status_code     = 200;
    let message         = 'Get datasets data success.';
    let result          = null;
    let missingParams   = [];

    let tags            = !_.isNil(input.tags)          ? JSON.parse(input.tags)		: null;
    let frequencies     = !_.isNil(input.frequencies)   ? JSON.parse(input.frequencies)	: [];
	let source     		= !_.isNil(input.source)   		? JSON.parse(input.source)		: [];

	let explain     	= !_.isNil(input.explain)   	? (input.explain == 'true')		: false;

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
                { $match: { freqs: { $in: frequencies }, tags: { $in: tags }, source : { $in: source }}},
                { $project : {
                    name: '$dataset',
					tags: { $filter: { input: '$tags', as: 'tag', cond: { $in: ['$$tag', tags]}}},
					frequency: { $filter: { input: '$freqs', as: 'freq', cond: { $in: ['$$freq', frequencies]}}},
                }},
            ], { explain }, (err, res) => {
				if (err) { return flowCallback(err); } else {
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
