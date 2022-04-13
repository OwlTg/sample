"use strict";

import {GenerateOptOutLinkParams, GenerateViewOnlineLinkParams, LinkGenerator} from "./link-generator";

require('dotenv').config();

const linkGenerator = require("./link-generator");
import {nanoid} from 'nanoid';
import {S3Transport} from "./transport/s3.transport";
import {BaseObject} from "./base.object";
import {StatCollector} from "./stats/stat.collector";
const statCollector: StatCollector = StatCollector.getInstance();

const short = require('short');

short.connect(process.env.MONGO_HASH);
short.connection.on('error', function(error) {
    throw new Error(error);
});
let s3Transport = new S3Transport(process.env.AWS_S3_VIBER_LINKS_BUCKET);
let Memcached = require('memcached');
let memcached;
memcached = new Memcached(process.env.MEMCACHED);

describe('LinkGenerator', () => {
    const mockedPutObject = jest.fn();
    jest.mock('aws-sdk/clients/s3', () => {
        return class s3Transport {
            putObject(params, cb) {
                mockedPutObject(params, cb);
            }
        }
    });
    beforeEach(() => {
        this.linkGenerator = new LinkGenerator();
    });
    describe("generateOptOutLink", () => {

        it("should generate OtpOut link", async () => {

            let data: GenerateOptOutLinkParams = {
                sender_id: 'Promotexter',
                mobile_number: '639568442768',
                id: 'transactionId',
                campaign_id: '111111',
                account_id: 1
            };
            // @ts-ignore
            this.linkGenerator.addMemcached = jest.fn(() => {
                return Promise.resolve()
            });


            // @ts-ignore
            this.linkGenerator.getNanoid = jest.fn(() => {
                return 'Aopt-out';
            });

            // @ts-ignore
            this.linkGenerator.uploadObject = jest.fn(() => {
            });


            let link = await this.linkGenerator.generateOptOutLink('[optout-shorturl-link]', data)

            expect(this.linkGenerator.getNanoid).toHaveBeenCalled();
            expect(this.linkGenerator.uploadObject).toHaveBeenCalledTimes(1);
            expect(link).toEqual('http://localhost:8096/Aopt-out ');

        });

        it("should generate OtpOut link for old place holder", async () => {

            let data: GenerateOptOutLinkParams = {
                sender_id: 'Promotexter',
                mobile_number: '639568442768',
                id: 'transactionId',
                campaign_id: '111111',
                account_id: 1
            };

            // @ts-ignore
            short.generate = jest.fn(() => {
                return {hash : 'optout' };
            });


            let link = await this.linkGenerator.generateOptOutLink('[optout-shortur-link]', data)

            expect(short.generate).toHaveBeenCalled();
            expect(link).toEqual('http://localhost:8096/optout ');

        });

        it("should not generate OtpOut link with wrong shorturl", async () => {

            let data = {
                sender_id: 'Promotexter',
                mobile_number: '639568442768',
                id: 'transactionId',
                campaign_id: '111111',
                account_id: 1
            };
            // @ts-ignore
            this.linkGenerator.addMemcached = jest.fn(() => {
                return Promise.resolve()
            });

            // @ts-ignore
            this.linkGenerator.getNanoid = jest.fn(() => {
                return 'brown';
            });

            // @ts-ignore
            s3Transport.headObject = jest.fn(() => {
                throw new Error('Nanoid not yet exists in s3')
            });
            // @ts-ignore
            s3Transport.putObject = jest.fn(() => {
                return {
                    promise: () => {
                    }
                };
            });


            let link = await this.linkGenerator.generateOptOutLink('lorem ipsum', data)
            expect(this.linkGenerator.getNanoid).toHaveBeenCalledTimes(0);
            expect(s3Transport.headObject).toHaveBeenCalledTimes(0);
            expect(s3Transport.putObject).toHaveBeenCalledTimes(0);
            expect(link).toEqual('lorem ipsum');

        });

        it("should replace  lastChar when -", async () => {
            let char = '1234567-'
            // @ts-ignore
            this.linkGenerator.randomChar = jest.fn(() => {
                return 'A';
            });

            let lastChar = this.linkGenerator.randomChar(1);
            char = '1234567' + lastChar

            expect(this.linkGenerator.randomChar).toHaveBeenCalled();
            expect(char).toEqual('1234567A');
        });
        it("should not replace  lastChar when ending not in -", async () => {
            let char = '12345678'
            // @ts-ignore
            this.linkGenerator.randomChar = jest.fn(() => {
                return 'A';
            });
            let newChar = this.linkGenerator.randomChar(1);
            let lastChar = char.substring(char.length - 1 , char.length)
            if(lastChar == '_' || lastChar == '-'){
                char = '1234567' + newChar
            }
            expect(this.linkGenerator.randomChar).toHaveBeenCalled();
            expect(char).toEqual('12345678');
        });
        it("should replace  lastChar when _ or -", async () => {
            let char = '1234567_'
            // @ts-ignore
            this.linkGenerator.randomChar = jest.fn(() => {
                return 'A';
            });

            let lastChar = this.linkGenerator.randomChar(1);
            char = '1234567' + lastChar

            expect(this.linkGenerator.randomChar).toHaveBeenCalled();
            expect(char).toEqual('1234567A');
        });

        it( "test api kickbox", async () => {


            let char = this.linkGenerator.checkKickboxBalance();


            expect(char).toEqual('1234567A');
        });


    });

    describe("generateViewOnlineLink", () => {

        it("should generate View Online link", async () => {

            let data: GenerateViewOnlineLinkParams = {
                mobile_number: '639568442768',
                id: 'transactionId',
                campaign_id: '111111',
                account_id: 1,
                personalData: {"email": null, "mobile_number": "639568442768", "name": null}
            };
            // @ts-ignore
            this.linkGenerator.addMemcached = jest.fn(() => {
                return Promise.resolve()
            });

            // @ts-ignore
            this.linkGenerator.getNanoid = jest.fn(() => {
                return 'viberlin';
            });

            // @ts-ignore
            s3Transport.headObject = jest.fn(() => {
                throw new Error('Nanoid not yet exists in s3')
            });
            // @ts-ignore
            this.linkGenerator.uploadObject = jest.fn(() => {
            });


            let link = await this.linkGenerator.generateViewOnlineLink('[view-online-short-url]', data)

            expect(this.linkGenerator.getNanoid).toHaveBeenCalled();
            expect(this.linkGenerator.uploadObject).toHaveBeenCalledTimes(1);
            expect(link).toEqual('http://localhost:8096/viberlin ');

        });

        it("should not generate View Online link with wrong shorturl", async () => {

            let data: GenerateViewOnlineLinkParams = {
                mobile_number: '639568442768',
                id: 'transactionId',
                campaign_id: '111111',
                account_id: 1,
                personalData: {"email": null, "mobile_number": "639568442768", "name": null}
            };

            // @ts-ignore
            this.linkGenerator.getNanoid = jest.fn(() => {
                return 'Zviberli';
            });

            // @ts-ignore
            s3Transport.headObject = jest.fn(() => {
                throw new Error('Nanoid not yet exists in s3')
            });
            // @ts-ignore
            s3Transport.putObject = jest.fn(() => {
                return {
                    promise: () => {
                    }
                };
            });


            let link = await this.linkGenerator.generateViewOnlineLink('lorem ipsum', data)
            expect(this.linkGenerator.getNanoid).toHaveBeenCalledTimes(0);
            expect(s3Transport.headObject).toHaveBeenCalledTimes(0);
            expect(s3Transport.putObject).toHaveBeenCalledTimes(0);
            expect(link).toEqual('lorem ipsum');

        });



    });


});
