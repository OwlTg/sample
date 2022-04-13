'use strict';

var _ = require('lodash');
var request = require('request');
var qs = require('querystring');
var config = require('../../config/environment');
var Logger = require("logger");
var logger = new Logger("mdbx.io");
var short = require('short');
short.connect(config.mongo.uri);
short.connection.on('error', function(error) {
  throw new Error(error);
});

var AWS = require('aws-sdk');
AWS.config.update({
  accessKeyId: config.mdbxAws.accessKeyId,
  secretAccessKey: config.mdbxAws.secretAccessKey,
  region: config.mdbxAws.region,
});
var s3 = new AWS.S3();

exports.viewOnlineViberOrionGet = function(req, res) {
  logger.info('apiEndPoint', config.apiEndPoint + '/public/viber/view_online_link_orion?' + qs.stringify(req.query));
  request.get({
    url: config.apiEndPoint + '/public/viber/view_online_link_orion?' + qs.stringify(req.query),
    headers: {
      Origin: config.origin || 'https://portal.promotexter.com'
    }
  }, function(err, http, body) {
    if(err) {
      logger.info('http', http);
      logger.error("err", err);
    } else {
      logger.info("body", body);
      var response = {};
      try {
        response = JSON.parse(body);
        if(response.message != 'Expired'){ // Get Personal data if not expired
          if(req.query.hash.length == '8') { /* If New Link, expired links are validated in route already */
            var params = {
              Bucket: config.mdbxAws.bucket,
              Key: req.query.hash + '/data.json'
            };
            try{
              // get data.json file in s3 and parse it on url
              s3.getObject(params, function (err, data) {
                var jsonDecode = JSON.parse(data.Body.toString());
                if(typeof jsonDecode.personalData !== 'undefined'){
                  response.data.personalData = JSON.parse(jsonDecode.personalData);
                }
                res.json(response);
              });
            }catch(s3err){
              logger.error("Error in s3 get Personal Data", s3err);
              res.send(400);
            }
          } else { /* If Old Link, need to check expire column in mongodb */
            short.retrieve(req.query.hash).then(function(data) {
              if(JSON.parse(data.URL).personalData && response.message !== 'Expired'){
                response.data.personalData = JSON.parse(JSON.parse(data.URL).personalData);
              }
              res.json(response);
            });
          }
        } else { // link expired
          res.json(response);
        }
      } catch(e) {
        logger.error("JSON.parse", config.apiEndPoint, e.message);
        res.send(400);
      }
    }
  });
};
