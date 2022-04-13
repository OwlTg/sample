/*
 * Copyright (c) 2017.
 * Created by Francis Vianney Salvamante
 * Junior Developer
 * Promotexter Philippines Inc.
 *
 */

var short = require('short');
var sha1 = require('sha1');
var config = require('../../config/environment/');

short.connect(config.mongo.uri);
short.connection.on('error', function(error) {
    throw new Error(error);
});

var AWS = require('aws-sdk');
const Logger = require("logger");
AWS.config.update({
  accessKeyId: config.mdbxAws.accessKeyId,
  secretAccessKey: config.mdbxAws.secretAccessKey,
  region: config.mdbxAws.region,
});
var s3 = new AWS.S3();
var logger = new Logger("mdbx.io");

exports.index = function(req, res) {
    short.retrieve(req.params.hash).then(function(result) {
        var jsonDecode = JSON.parse(result.URL);

        var sha = sha1(jsonDecode.id + jsonDecode.mobile_number + config.encryption_key);

        if(jsonDecode.version === 'orion'){
            if(jsonDecode.type === 'view-online')
            {
              res.redirect('/view-online-viber-orion?key=' + jsonDecode.key + '&id=' + jsonDecode.id + '&campaignId=' + jsonDecode.campaign_id + '&mobile=' + jsonDecode.mobile_number + '&hash=' + req.params.hash + '&type=' + jsonDecode.type);
            }else{
              res.redirect('/unsubscribe-sms-orion?key=' + jsonDecode.key + '&id=' + jsonDecode.id + '&categoryId=' + jsonDecode.campaign_id + '&accountId=' + jsonDecode.account_id + '&mobile=' + jsonDecode.mobile_number + '&sender=' + jsonDecode.sender_id);
            }
        }else{
            res.redirect('/unsubscribe-sms?key=' + sha + '&id=' + jsonDecode.id);
        }

    }, function (err) {
        res.redirect('/link-expired');
    });
};

exports.handleNewLink = function (req, res) {
  var url = req.originalUrl;
  var identifier = url.lastIndexOf('/');
  var hash = url.substring(identifier + 1, url.length);

  var params = {
    Bucket: config.mdbxAws.bucket,
    Key: hash + '/data.json'
  };


  // get data.json file in s3 and parse it on url
  s3.getObject(params, function (err, data) {
    if (err) { /* does not exists in s3 folder */
      res.redirect('/link-expired');
      logger.error('Error Fetching from s3 Viber Links',err);
    } else {
      logger.info('Success Fetching data', data);
      var jsonDecode = JSON.parse(data.Body.toString());
      var sha = sha1(jsonDecode.id + jsonDecode.mobile_number + config.encryption_key);
      if (jsonDecode.version === 'orion') {
        if (jsonDecode.type === 'view-online') {

          res.redirect('/view-online-viber-orion?key=' + jsonDecode.key + '&id=' + jsonDecode.id + '&campaignId=' + jsonDecode.campaign_id + '&mobile=' + jsonDecode.mobile_number + '&hash=' + req.params.hash + '&type=' + jsonDecode.type);
        } else {
          res.redirect('/unsubscribe-sms-orion?key=' + jsonDecode.key + '&id=' + jsonDecode.id + '&categoryId=' + jsonDecode.campaign_id + '&accountId=' + jsonDecode.account_id + '&mobile=' + jsonDecode.mobile_number + '&sender=' + jsonDecode.sender_id);
        }
      } else {
        res.redirect('/unsubscribe-sms?key=' + sha + '&id=' + jsonDecode.id);
      }

    }
  });

};
