let express         = require('express');
let bodyParser      = require('body-parser');
let cookieParser    = require('cookie-parser');
let cors            = require('cors');

let app             = express();

app.use(bodyParser.json({ limit: '200mb' }));
app.use(bodyParser.urlencoded({ limit: '200mb', extended: true }));
app.use(cookieParser());
app.use(cors());

app.use(express.static(__dirname + '/public'));
app.use('/', require('./router'));

// catch 404 and forward to error handler
app.use(function(req, res, next) {
    res.setHeader('Content-Type', 'application/json');
    res.status(404).send(JSON.stringify({
        response        : "FAILED",
        statusCode      : 404,
        result          : null,
        message         : "Whooops! Where are you going?",
    }));
});

// error handlers

// development error handler
// will print stacktrace
if (process.env.APP_ENV === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.json({
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to operator
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.json({
        message: err.message,
        error: {}
    });
});

module.exports = app;
