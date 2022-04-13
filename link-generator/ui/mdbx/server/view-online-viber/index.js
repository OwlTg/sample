'use strict';

var express = require('express');
var controller = require('./view-online-viber.controller');

var router = express.Router();

router.get('/view-online-viber-orion-get', controller.viewOnlineViberOrionGet);

module.exports = router;
