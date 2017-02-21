let MongoClient = require('mongodb').MongoClient

let state = { db : null }

exports.connect = function(url, callback) {
    if (state.db) return callback();

    MongoClient.connect(url, function(err, db) {
        if (err) return callback(err);
        state.db    = db;
        callback();
    })
}

exports.get = function() { return state.db; }

exports.close = function(callback) {
    if (state.db) {
        state.db.close(function(err, result) {
            state.db = null;
            state.mode = null;
            callback(err);
        })
    }
}
