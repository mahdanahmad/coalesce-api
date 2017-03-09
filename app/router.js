const express       = require('express');
const router        = express.Router();

const controller    = require('./controllers/index');

/* index. */
router.get('/', (req,res, next) => { res.json() });
router.get('/config', (req,res, next) => {
    controller.config((result) => { res.status(result.status_code).json(result); });
});
router.get('/selector', (req, res, next) => {
    controller.selector(req.query, (result) => { res.status(result.status_code).json(result); });
});
router.get('/stacked', (req, res, next) => {
    controller.stacked(req.query, (result) => { res.status(result.status_code).json(result); });
});
router.get('/datasets', (req, res, next) => {
    controller.datasets(req.query, (result) => { res.status(result.status_code).json(result); });
});

module.exports = router;
