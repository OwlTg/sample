/*
 * Copyright (c) 08/23/21 07:01 PM.
 * Created by OwenPTX
 * Engineer
 * Promotexter Philippines Inc.
 */

import {nanoid} from 'nanoid';
import {OrionHealth, StatCollector} from "./stats/stat.collector";
import {S3Transport} from "./transport/s3.transport";
import * as crypto from "crypto";
import {NEW_OPT_OUT_LINK, OLD_OPT_OUT_LINK, VIEW_ONLINE_LINK} from "../constants";
import * as AWS from "aws-sdk";
import {exitOnError} from "winston";

let Memcached = require('memcached');
let memcached;
memcached = new Memcached(process.env.MEMCACHED);

const statCollector: StatCollector = StatCollector.getInstance();

const short = require('short');

short.connect(process.env.MONGO_HASH);
short.connection.on('error', function (error) {
    throw new Error(error);
});


export interface GenerateOptOutLinkParams {
    id: string,
    mobile_number: string,
    account_id: number,
    sender_id: string,
    campaign_id: string,
}

export interface GenerateViewOnlineLinkParams {
    id: string,
    mobile_number: string,
    account_id: number,
    campaign_id: string,
    personalData: any
}

export class LinkGenerator {
    s3Transport: S3Transport;

    constructor() {
        this.s3Transport = new S3Transport(process.env.AWS_S3_VIBER_LINKS_BUCKET);

    }

    async generateOptOutLink(text: string, data: GenerateOptOutLinkParams): Promise<string> {
        let sha = crypto.createHash('sha1');
        sha.update(data.id + data.mobile_number + process.env.SALT_FOR_UNSUBSCRIBE);
        var startTime = new Date();
        var nanoId, mongodbDoc;
        let json = {
            id: data.id,
            key: sha.digest('hex'),
            mobile_number: data.mobile_number,
            account_id: data.account_id,
            sender_id: data.sender_id,
            campaign_id: data.campaign_id,
            version: 'orion'
        };

        if (text.indexOf(OLD_OPT_OUT_LINK) !== -1) { /* old links*/
            mongodbDoc = await short.generate({
                URL: JSON.stringify(json)
            });
            if (mongodbDoc.hash !== '') { /* old links */
                text = text.replace(OLD_OPT_OUT_LINK, process.env.MDBX_URL + mongodbDoc.hash + ' ');
            }
        }

        if (text.indexOf(NEW_OPT_OUT_LINK) !== -1) { /* new links*/
            //try 1000 tries until generate unique
            for (let i = 0; i < 1000; i++) {
                nanoId = this.getNanoid();
                let lastChar = nanoId.substring(nanoId.length - 1, nanoId.length)
                if(lastChar == '_' || lastChar == '-'){ // replace last char if - or _
                    let editedNanoId = nanoId.slice(0, -1)
                    nanoId = editedNanoId + this.randomChar(1);
                }
                let destination = nanoId + '/data.json';

                let memcachedRes = await this.addMemcached(nanoId, '', parseInt(process.env.MEMCACHED_VIBER_LINK_EXPIRATION))
                    .then(data =>{
                        return true;
                    }).catch(err =>{
                        return false;
                    })

                if(memcachedRes){ /* if memcache has no error, continue >> */
                    //check file in s3
                    try {
                        // hash already exists in s3 should retry generating hash
                        let chekFileStatus = await this.checkObject(process.env.AWS_S3_VIBER_LINKS_BUCKET, destination);

                    } catch (headErr) {
                        if (headErr.code === 'NotFound') {
                            // Upload to S3 if no similar link found
                            let response = await this.uploadObject(process.env.AWS_S3_VIBER_LINKS_BUCKET, destination, JSON.stringify(json));
                            break;
                        }

                    }
                }

                // log in influx all generated links
                statCollector.add('viber.optoutlink.exists', {
                    duration: new Date().getTime() - startTime.getTime(),
                }, {
                    accountId: data.id,
                    key: nanoId,
                    mobile_number: data.mobile_number,
                    account_id: data.account_id,
                    sender_id: data.sender_id,
                    campaign_id: data.campaign_id
                });
                nanoId = '';

            }

            if (nanoId !== '') { /* new links */
                text = text.replace(NEW_OPT_OUT_LINK, process.env.MDBX_URL + nanoId + ' ');
            } else {
                throw new Error('Cannot generate Hash for Opt-Out Link');
            }
            return text;

        } else {
            return text;
        }

    }

    async generateViewOnlineLink(text: string, data: GenerateViewOnlineLinkParams): Promise<string> {

        if (text.indexOf(VIEW_ONLINE_LINK) === -1) return text;
        let sha = crypto.createHash('sha1');
        let type = 'view-online';
        sha.update(data.id + data.mobile_number + type + process.env.SALT_FOR_UNSUBSCRIBE);
        let personalData = JSON.stringify(data.personalData);
        var startTime = new Date();
        var nanoId;

        //try 1000 tries until generate unique
        for (let i = 0; i < 1000; i++) {
            nanoId = this.getNanoid();
            let lastChar = nanoId.substring(nanoId.length - 1, nanoId.length)
            if(lastChar == '_' || lastChar == '-'){ // replace last char if - or _
                let editedNanoId = nanoId.slice(0, -1)
                nanoId = editedNanoId + this.randomChar(1);
            }
            let destination = nanoId + '/data.json';

            let memcachedRes = await this.addMemcached(nanoId, '', parseInt(process.env.MEMCACHED_VIBER_LINK_EXPIRATION))
                .then(data =>{
                    return true;
                }).catch(err =>{
                    return false;
                })

            if(memcachedRes){ /* if memcache has no error, continue >> */
                //check file in s3
                try {
                    // hash already exists in s3 should retry generating hash
                    let chekFileStatus = await this.checkObject(process.env.AWS_S3_VIBER_LINKS_BUCKET, destination);

                } catch (headErr) {
                    if (headErr.code === 'NotFound') {
                        let json = {
                            id: data.id,
                            key: sha.digest('hex'),
                            mobile_number: data.mobile_number,
                            personalData: personalData,
                            campaign_id: data.campaign_id,
                            version: 'orion',
                            type: type
                        };

                        // Upload to S3 if no similar link found
                        let response = await this.uploadObject(process.env.AWS_S3_VIBER_LINKS_BUCKET, destination, JSON.stringify(json));
                        break;
                    }

                }
            }

            // log in influx all generated links
            statCollector.add('viber.onlinelink.exists', {
                duration: new Date().getTime() - startTime.getTime(),
            }, {
                accountId: data.id,
                key: nanoId,
                mobile_number: data.mobile_number,
                personalData: personalData,
                campaign_id: data.campaign_id,
                type: type
            });
            nanoId = '';
        }


        if (nanoId !== '') {
            let message = text.replace(VIEW_ONLINE_LINK, process.env.MDBX_URL + nanoId + ' ');
            return message;
        } else {
            throw new Error('Cannot generate Hash for View Online Link');
        }

    }

    async checkKickboxBalance(){
        var request = require('request');

        var options = {
            url: 'https://api.kickbox.com/v2/balance?apikey=caa1e7ee1720bc3094108b20611108b5bb3d759ab62b72d6e3170cdd210f741a'
        };

        request(options,  function(error, response, body) {
            if (!error && response.statusCode == 200) {
                let json = JSON.parse(response.body)
                if(json.balance > 0){
                   // logger.error("Kickbox Out of Credit", json)
                }
                console.log(json.balance);
            }
        });
    }

    getNanoid() {
        return nanoid(8)
    }
    randomChar(length: number){
        var result           = '';
        var characters       = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        var charactersLength = characters.length;
        for ( var i = 0; i < length; i++ ) {
            result += characters.charAt(Math.floor(Math.random() *
                charactersLength));
        }
        return result;
    }
    async checkObject(bucket = process.env.AWS_S3_VIBER_LINKS_BUCKET, destination):Promise<object> {
        return this.s3Transport.headObject(process.env.AWS_S3_VIBER_LINKS_BUCKET, destination);
    }

    async uploadObject(bucket = process.env.AWS_S3_VIBER_LINKS_BUCKET, destination, data):Promise<object> {
        return this.s3Transport.putObject(bucket, destination, data);
    }

    async addMemcached (key, value, time = parseInt(process.env.MEMCACHED_VIBER_LINK_EXPIRATION)):Promise<object> {
        return new Promise((resolve, reject) => {
            memcached.add(key, value, time, function (err, data){
                if(err){
                    reject(err)
                }
                if(data){
                    resolve(data)
                }
            })
        });
    }

}