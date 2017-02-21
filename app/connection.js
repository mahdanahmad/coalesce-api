const MongoDB		= require('mongodb');
const MongoClient	= MongoDB.MongoClient;
const ObjectID		= MongoDB.ObjectID;

const state 	= { db: null };

exports.connect = (url, callback) => {
	if (state.db) return callback();

	MongoClient.connect(url, (err, db) => {
		if (err) return callback(err);
		state.db    = db;
		callback();
	});
};

exports.close = (callback) => {
	if (state.db) {
		state.db.close((err, result) => {
			state.db = null;
			state.mode = null;
			callback(err);
		});
	}
};

exports.get 			= () => (state.db);
exports.toObjectID      = (stringID) => (ObjectID.isValid(stringID) ? new ObjectID(stringID) : null);
exports.getCollection   = (table) => (state.db.collection(table));
