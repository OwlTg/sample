import {ServiceBroker} from "moleculer";
import {AccountDao} from "../../lib/daos/account.dao";

import {SmsPojo} from "../../lib/models/pojo/sms.pojo";
import {TransactionChannelType, TransactionType} from "../../lib/daos/transaction.dao";
import {BaseController} from "../../lib/base.controller";
import {Pt3ApiDao} from "./daos/pt3api.dao";
import {InvalidSendEmailRequestError, InvalidSendSMSRequestError, InvalidSendViberRequestError,} from "./pt3.errors";
import {JobQueueFactory, parseQueueOptions} from "../../lib/jobqueue/job.queue.factory";
import {JobQueue, TypedJob} from "../../lib/jobqueue/job.queue";
import {OrionHealth, StatCollector} from "../../lib/stats/stat.collector";
import {GetApiTrafficReportParams, Pt3ReportDao} from "./daos/pt3report.dao";

import {EmailTransport} from "../../lib/transport/email.transport";
import * as _ from "lodash";
import {RouteDao} from "../../lib/daos/route.dao";
import {ReportDao} from "../../lib/daos/report.dao";
import {SmsController} from "../sms/sms.controller";
import {CampaignDao, CampaignTransactionSourceResultParams} from "./daos/campaign.dao";
import {
    CampaignStatsPojo,
    EmailCampaignTransactionRequest,
    OrionCampaignStats,
    SMSCampaignTransactionRequest,
    ViberCampaignTransactionRequest
} from "./models/campaign";
import {Contact, ContactDao, ContactStatus} from "./daos/contact.dao";
import {FileDao} from "./daos/file.dao";
import {S3Transport} from "../../lib/transport/s3.transport";
import {
    getFormattedCampaignIdForLogs,
    getFormattedCampaignIdForQeueuingLogs,
    getFormattedCampaignIdForSendingLogs, getFormattedCampaignIdForStatsLogs,
    interpolateMessage,
    renameFields
} from "../../lib/utils";
import {DlrEventHookType} from "../../lib/dlreventhook_strategy/dlr.event.hook.type";
import {SendingEventHookType} from "../../lib/sendingeventhook_strategy/sending.event.hook.type";
import {CampaignStatsDao} from "./daos/campaignstats.dao";
import {
    CampaignSourceParams,
    CampaignSourceStrategyFactory,
    CampaignSourceType,
    ConvertXLStoCSVParams
} from "../../lib/campaign_source_strategy/campaign.source.strategy";
import {CampaignTransactionModelTransformationStream} from "../../lib/streams/campaign.transaction.model.transformation.stream";
import {CampaignJobQueueDestinationStream} from "../../lib/streams/campaign.job.queue.destination.stream";
import {CacheProvider} from "../../lib/cachers/cache.provider";
import * as AWS from 'aws-sdk';
import {CampaignStatus, CampaignStatusLog, CampaignTypeCodeEnum} from "./enum/campaign.enum";
import {FileTypeCodeEnum} from "./enum/file.enum";
import {Campaign, CampaignLivePojo, CampaignSlackUpdateJob, PrepCampaignParams} from "./interfaces/campaign.interface";
import {EmailController} from "../email/usecases/email.controller";
import {SmsCampaignTransactionDestinationStream} from "../../lib/streams/sms-campaign-transaction-destination.stream";
import {EmailCampaignTransactionDestinationStream} from "../../lib/streams/email-campaign-transaction-destination.stream";
import {clearCachedCall} from "../../lib/decorators/cached.call.decorator";
import {EmailNotificationEnum} from "../../lib/models/emailnotification.enum";

import * as moment from 'moment-timezone';
import {ServiceCode} from "../service.interface";
import {EventEmitter} from "events";
import * as crypto from "crypto";
import {
    CAMPAIGN_CURRENT_PRIORITY_CACHE_KEY,
    CAMPAIGN_CURRENT_PRIORITY_CACHE_TTL,
    colors,
    NEW_OPT_OUT_LINK, OLD_OPT_OUT_LINK, VIBER_CAMPAIGN_CURRENT_PRIORITY_CACHE_KEY
} from "../../constants";
import {CampaignSlackUpdateDao} from "./daos/campaign.slack.update.dao";
import {ViberPojo} from "../../lib/models/pojo/viber.pojo";
import {ViberCampaignTransactionDestinationStream} from "../../lib/streams/viber-campaign-transaction-destination.stream";
import {ViberDao} from "../../lib/daos/viber.dao";
import Bluebird = require("bluebird");
import {
    LinkGenerator,
    GenerateOptOutLinkParams,
    GenerateViewOnlineLinkParams,
} from "../../lib/link-generator"


require("dotenv").config();

const uuid = require('uuid/v4');
const short = require('short');

const statCollector: StatCollector = StatCollector.getInstance();

export enum GenerateReportTypesEnum {
    LOGS = "Transaction Logs",
    MONTHLY = "Monthly Summary",
    DAILY = "Daily Summary",
    MOBILE = "Mobile Search"
}

export interface BasicMessageInterface {
    to: string
    from: string
    message: string,
    identifier: number,
}

export interface CampaignCompletionTracker {
    campaignId: number;
    totalSources: number;
}

export interface TrafficApiReportParams extends GetApiTrafficReportParams {
}

export namespace PT3 {




    interface ValidateSenderIdParams {
        accountId: number,
        senderidName: string
    }


    export interface ProcessCampaignParams {
        user?: any,
        id: number,
        requestId: string,
        transactionType: TransactionType,
        transactionChannelType: TransactionChannelType,
        from?: string,
        text?: string,
        categoryId?: any,
        groupIds?: number[],
        contactIds?: number[],
        fileIds?: number[],
        filter?: any
    }


    export interface GetListParams {
        user: {
            accountId: number,
            userId?: number,
        },

        pagination?: any,
        limit?: string,
        skip?: string,
        filter?: {
            dateCreatedGt?: string,
            dateCreatedLt?: string,
            referenceId?: string,
            requestId?: string,
            categoryId?: string,
            transactionId?: string,
            from?: string,
            to?: string,
            accountId?: number,
            userId?: number
        },
        sort?: {
            column: string,
            direction: string
        },
        pageSize?: number,
        pageNumber?: number
    }


    export interface DownloadParams extends GetListParams {
        format?: string,
        source?: string
    }

    export interface GetListData {
        status: boolean,
        totalRows: number,
        totalPages: number,
        pageNumber: number,
        hasPrevious: boolean,
        previousPage: number,
        hasNext: boolean,
        nextPage: number,
        pageSize: number,
        itemsOnPage: number,
        sortColumn: string,
        sortDirection: string,
        filter: {
            dateCreatedGt?: string,
            dateCreatedLt?: string,
            referenceId?: string,
            requestId?: string,
            categoryId?: string,
            transactionId?: string,
            keyword?: string,
        },
        data: Pt3ApiData[]
    }

    export interface Pt3ApiData {
        id?: string
        transactionId: string,
        requestId: string,
        categoryId?: string,
        categorySentByUser?: string,
        referenceId?: string,
        from: string,
        to: string,
        text: string,
        messageParts: number,
        price: number,
        dateCreated: string,
        dateSent: string,
        dlrStatus: any,
        sendStatus: number,
        sourceIp: string,
        operator: string,
        description: string,
        userdata1?: string,
        userdata2?: string,
        userdata3?: string,
        userdata4?: string,
        userdata5?: string
    }

    export interface GetListReportParams extends GetListParams {
        reportType?: string,
        dateFrom: Date,
        dateTo: Date,
        dateCreated: Date,
        dateExpired: Date,
        status?: string,
        is_valid: number,
        url_link?: string
    }

    export interface GetReportListData {
        status: boolean,
        totalRows: number,
        totalPages: number,
        pageNumber: number,
        hasPrevious: boolean,
        previousPage: number,
        hasNext: boolean,
        nextPage: number,
        pageSize: number,
        itemsOnPage: number,
        sortColumn: string,
        sortDirection: string,
        filter: {
            dateCreatedGt?: string,
            dateCreatedLt?: string,
            referenceId?: string,
            requestId?: string,
            categoryId?: string,
            transactionId?: string,
            keyword?: string,
        },
        data: Pt3ReportData[]
    }

    export interface Pt3ReportData {
        _id?: string,
        accountId: number,
        userId: number,
        user: User,
        reportType: string,
        dateFrom: Date,
        dateTo: Date,
        dateCreated: Date,
        dateExpired: Date,
        status: string,
        is_valid: number,
        url_link: string
    }


    export interface SmppApiData {
        id?: string
        transactionId: string,
        requestId: string,
        categoryId?: string,
        categorySentByUser?: string,
        referenceId?: string,
        from: string,
        to: string,
        text: string,
        messageParts: number,
        price: number,
        dateCreated: string,
        dateSent: string,
        dlrStatus: number,
        sendStatus: number,
        sourceIp: string,
        operator: string,
        description: string,
        accountName?: string,
        routeName?: string
    }


    interface User {
        userName
    }

    export interface CreateReportParams {
        accountId: number,
        userId: number,
        user: User,
        reportType: string,
        dateFrom: string,
        dateTo: string,
        keyword?: string,
        extra?: any,
        categoryId?: string,
        sort?: {
            direction?: string,
            column?: string
        },
        transactionType: TransactionType
    }

    export interface EtlParams {
        accountId: number,
        userId: number,
    }

    export interface CreateReportResponse {
        _id?: string,
        accountId: number,
        userId: number,
        user: User,
        reportId?: string,
        reportType: string,
        dateFrom: Date,
        dateTo: Date,
        dateCreated?: Date,
        dateExpired?: Date,
        status?: string,
        is_valid?: number,
        url_link?: string
    }


    export interface CreateReportResponse {
        _id?: string,
        accountId: number,
        userId: number,
        user: User,
        reportId?: string,
        reportType: string,
        dateFrom: Date,
        dateTo: Date,
        dateCreated?: Date,
        dateExpired?: Date,
        status?: string,
        is_valid?: number,
        url_link?: string
    }

    export class CampaignController extends BaseController {

        dao: Pt3ApiDao;
        jq: JobQueue;
        // writeQ: JobQueue;
        pt3ReportDao: Pt3ReportDao;
        reportDao: ReportDao;
        emailTransport: EmailTransport;
        accountDao: AccountDao;
        routeDao: RouteDao;
        smsCtrl: SmsController;
        campaignDao: CampaignDao;
        campaignStatsDao: CampaignStatsDao;
        contactDao: ContactDao;
        fileDao: FileDao;
        s3Transport: S3Transport;
        batchLimit: number;
        forceStop: boolean;
        sqs: AWS.SQS;
        xlsConverterQ: JobQueue;
        pt3CampaignTransactionSourceQ: JobQueue;
        pt3CampaignLiveQ: JobQueue;
        pt3CampaignBroadcastQ: JobQueue;
        pt3ViberTransactionSourceQ: JobQueue;
        short;
        campaignSlackUpdateDao: CampaignSlackUpdateDao;
        pt3CampaignSlackUpdateQ: JobQueue;
        recomputeTotalCostQ: JobQueue;
        viberDao: ViberDao;
        linkGenerator: LinkGenerator;

        constructor(name: string, broker?: ServiceBroker) {
            super(name, broker);
            this.dao = new Pt3ApiDao();
            // this.jq = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.PT3_SMS_CAMPAIGN_TRANSACTION_PROCESS_Q));
            this.emailTransport = new EmailTransport();
            this.accountDao = new AccountDao();
            this.routeDao = new RouteDao();
            this.viberDao = new ViberDao();
            // this.writeQ = JobQueueFactory.createJobQueue(process.env.PT3_SMS_WRITE_Q_TYPE);
            this.pt3CampaignTransactionSourceQ = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_Q));
            this.pt3ViberTransactionSourceQ = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.PT3_VIBER_TRANSACTION_SOURCE_Q));
            this.pt3CampaignLiveQ = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.PT3_CAMPAIGN_LIVE_Q));
            this.pt3CampaignBroadcastQ = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.PT3_BROADCAST_CAMPAIGN_QUEUE));
            this.pt3CampaignSlackUpdateQ = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.PT3_CAMPAIGN_SLACK_UPDATE_QUEUE));
            this.xlsConverterQ = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.PT3_CAMPAIGN_XLS_CONVERSION_QUEUE));
            this.recomputeTotalCostQ = JobQueueFactory.createJobQueue(parseQueueOptions(process.env.VIBER_RECOMPUTE_TOTAL_COST_Q));

            this.smsCtrl = new SmsController("campaignController", broker);

            // this.slack.send("[PT3SMS API] I'm up!");
            this.linkGenerator = new LinkGenerator();
            this.campaignDao = new CampaignDao();
            this.campaignStatsDao = new CampaignStatsDao();
            this.contactDao = new ContactDao();
            this.fileDao = new FileDao();
            this.campaignSlackUpdateDao = new CampaignSlackUpdateDao();
            this.s3Transport = new S3Transport(process.env.AWS_PT3_UPLOADS_BUCKET);
            this.sqs = new AWS.SQS({
                region: process.env.AWS_SQS_PT3_REGION,
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
            });

            this.batchLimit = 100;

            /***
             * This is receiving a forceStop signal from the runners, this breaks the while loop
             *
             * @type {boolean}
             */
            this.forceStop = false;

            short.connect(process.env.MONGO_HASH);
            short.connection.on('error', function(error) {
                throw new Error(error);
            });
        }



        async initializeCampaignStats(requestId: string, campaign: Campaign): Promise<OrionCampaignStats> {
            let campaignStats: OrionCampaignStats = await this.campaignDao.getCampaignStats(campaign.id, campaign.accountId);

            if (campaignStats) {
                this.logger.warn({
                    id: requestId,
                    message: "Campaign Stats already exists. This is a re-trigger",
                    value: campaignStats
                });

                return campaignStats;
            }
            else {
                return await this.campaignDao.createCampaignStats(requestId, campaign);
            }
        }

        async initializePt3CampaignStats(requestId: string, campaign: Campaign): Promise<CampaignStatsPojo> {
            let campaignStats: CampaignStatsPojo = await this.campaignStatsDao.getCampaignStats(campaign.id);

            if (campaignStats) {
                this.logger.warn({
                    id: requestId,
                    message: "Campaign Stats already exists. This is a re-trigger",
                    value: campaignStats
                });

                return campaignStats;
            }
            else {
                if(campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER) {
                    await this.campaignStatsDao.createViberCampaignStats({
                        campaignId: campaign.id
                    });
                }
                return await this.campaignStatsDao.createCampaignStats({
                    campaignId: campaign.id,
                    queued: campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER ? 0 : campaign.totalRecipients
                });
            }
        }

        /***
         *
         * All the business rules whether the contact must be part of the contacts is in here
         *
         * @param {ProcessCampaignParams} params
         * @param {Contact} contact
         * @returns {Promise<void>}
         */
        async validateRecipient(params: ProcessCampaignParams, contact: Contact): Promise<boolean> {

            if (!contact.isValid) {
                this.logger.info({id: params.requestId, message: "Invalid contact", value: contact});
                return false;
            }

            if (contact.mobileStatusId != ContactStatus.VALID) {
                this.logger.info({id: params.requestId, message: "Invalid mobile status", value: contact});
                return false;
            }


            // todo: These validations must be added instead on the core SMS service
            // queue_mobile_with_prefix_only
            // CQ_NO_PREFIX
            // $sql = "update campaign_bulk_q IGNORE INDEX (FK76C50A93B0DADB95) set status = FALSE, error_id = $error->id where campaign_id = $campaign->id and prefix_id is NULL";

            // queue_mobile_not_in_blocked_list
            // CQ_BLOCKED
            // $this->sql_query("create temporary table tmp_blocked_q{$campaign->id} as select q.id from blocked_list bl join tmp_campaign_bulk_q{$campaign->id} q on bl.contact = q.mobileNumber where bl.accountId = $campaign->accountId and bl.contact_type_id = $contact_type->id;");

            let blocked: Boolean = await this.contactDao.checkSmsRecipientIfBlocked(contact);
            if (blocked) {
                this.logger.info({
                    id: params.requestId,
                    message: "Contact is blocked",
                    value: {recipient: contact.mobileNumber}
                });
                return false;
            }


            // queue_mobile_not_in_unsubscribed_list
            // CQ_OPTED_OUT
            // $this->sql_query("create temporary table tmp_unsubscribed_q{$campaign->id} as select cbq.id from unsubscribe_list ul join tmp_campaign_bulk_q{$campaign->id} cbq on ul.isValid = true and ul.contact = cbq.mobileNumber where ul.senderid = cbq.sender_name and ul.accountId = $campaign->accountId and ul.contact_type_id = $contact_type->id;");

            let unsubscribed: Boolean = await this.contactDao.checkSmsRecipientIfUnsubscribed(contact, params.from);
            if (unsubscribed) {
                this.logger.info({
                    id: params.requestId,
                    message: "Contact has Unsubscribed",
                    value: {recipient: contact.mobileNumber}
                });
                return false;
            }

            // queue_allow_international_sms
            // CQ_INT_SMS_ALLOW
            // $account->has_international_sms

            return true;
        }

        async convertXLSToCSV(payload: ConvertXLStoCSVParams) {
            return new Promise(async (resolve, reject) => {
                let temporaryFile = this.getTemporaryFilename();
                // let csvReadStream,  writableStream;
                try {
                    // csvReadStream = await this.s3Transport.createReadStream(payload.s3Bucket, payload.s3Key);
                    // writableStream = await fs.createWriteStream(temporaryFile);
                    await this.s3Transport.saveToLocalFile(payload.s3Bucket, payload.s3Key, temporaryFile, payload.fileHash);
                } catch(e) {
                    return reject(e);
                }

                // csvReadStream.pipe(writableStream).on('finish', async () => {
                try {
                    await this.s3Transport.convertXLSToCsv(temporaryFile, payload.outputFilepath);
                    await this.s3Transport.deleteLocally(temporaryFile);

                    let chunks = await this.s3Transport.chunkCsvFile(payload.outputFilepath, parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_FILE_ROW_BATCH_SIZE));

                    if (chunks !== payload.sourceJobs.length) {
                        this.logger.error({
                            message: "Number of chunked files does not match number of jobs",
                            id: getFormattedCampaignIdForQeueuingLogs(payload.campaignId),
                            value: {
                                chunks,
                                jobs: payload.sourceJobs.length
                            }
                        });
                        return reject(new Error('Number of chunked files does not match number of jobs'));
                    }

                    await Promise.all(payload.sourceJobs.map(async (job) => {
                        await this.pt3CampaignTransactionSourceQ.publish(job, {priority: job.priority});
                        this.logger.info({
                            message: 'Sent to campaign transaction source queue',
                            id: getFormattedCampaignIdForLogs(payload.campaignId),
                            value: job
                        });
                    }));

                } catch (e) {
                    this.logger.error({
                        message: 'convertXLSToCsv error encountered',
                        reason: e.message,
                        value: {stack: e.stack}
                    });
                    await this.s3Transport.deleteLocally(temporaryFile);
                    return reject(e);
                }

                resolve(true);
                // }).on('error', (e) => {
                //     this.logger.error({message: 'convertXLSToCSV pipe error encountered', reason: e.message, value: {stack: e.stack}});
                //     reject(e);
                // });

                // csvReadStream.on('error', (e) => {
                //     reject(e);
                // })

            });
        }

        getTemporaryFilename() {
            let now = new Date();
            return './uploads/campaign-file-' + now.toLocaleDateString().replace(/\//g, '-') + '-' + uuid.v4();
        }

        async processTransactionSource(payload: CampaignSourceParams, eventEmitter: EventEmitter): Promise<CampaignTransactionSourceResultParams> {

            let campaign: Campaign ;
            if(payload.campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER){
                campaign = await this.campaignDao.getViberCampaign(payload.campaign.id, payload.campaign.accountId);
            }else{
                campaign = await this.campaignDao.getCampaign(payload.campaign.id, payload.campaign.accountId);
            }

            if (campaign.campaignStatusId === CampaignStatus.LIVE) {
                let ts = new Date();
                let campaignSourceStrategy = this.getCampaignSourceStrategy(payload.source.type);
                let jobQueue = this.getJobQueue(payload.queueUrl);
                let transformStream: any = this.getCampaignTransactionModelTransformationStream(payload);
                let mongoStream: any = this.getCampaignTransactionDestinationStream(payload);
                let queueStream: any = this.getJobQueueDestinationStream(jobQueue);

                let cache = await this.getCacheDriver();
                if(payload.campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER){
                    await cache.set(VIBER_CAMPAIGN_CURRENT_PRIORITY_CACHE_KEY, payload.priority, CAMPAIGN_CURRENT_PRIORITY_CACHE_TTL);
                }else{
                    await cache.set(CAMPAIGN_CURRENT_PRIORITY_CACHE_KEY, payload.priority, CAMPAIGN_CURRENT_PRIORITY_CACHE_TTL);
                }

                let key = process.env.PT3_CAMPAIGN_QUEUEING_CACHE_NAME.replace(/id$/, '' + payload.campaign.id);
                let detectedPause = false, detectedShutdown = false;

                let readableStream = await campaignSourceStrategy.getReadableStream(payload);

                eventEmitter.once('GRACEFUL SHUTDOWN', () => {
                    this.logger.info({
                        message: 'Campaign process transaction stopped',
                        id: getFormattedCampaignIdForLogs(payload.campaign.id),
                        value: {
                            campaignStatusId: campaign.campaignStatusId,
                            reason: 'GRACEFUL SHUTDOWN'
                        }
                    });
                    detectedShutdown = true;
                    transformStream.stop();
                    if (typeof (<any>readableStream).destroy === 'function')
                        (<any>readableStream).destroy();
                });

                let interval = setInterval(async () => {
                    // this.logger.info('Set Campaign still queueing to true');
                    var total = transformStream.getCount().total;
                    var processed = transformStream.getCount().processed;
                    var skipped = transformStream.getCount().skipped;
                    var duplicate = mongoStream.getCount().duplicate;
                    var dropped = mongoStream.getCount().dropped;
                    var queued = queueStream.getCount().queued;
                    var saved = mongoStream.getCount().saved;
                    var totalParts = transformStream.getCount().totalParts;
                    var droppedParts = mongoStream.getCount().droppedParts;
                    var queuedParts = queueStream.getCount().queuedParts;
                    var savedParts = mongoStream.getCount().savedParts;
                    var invalid = mongoStream.getCount().invalid;
                    var invalidParts = mongoStream.getCount().invalidParts;
                    var duplicateParts = mongoStream.getCount().duplicateParts;

                    this.logger.info({
                        message: 'Campaign process transaction source progress',
                        id: getFormattedCampaignIdForQeueuingLogs(payload.campaign.id),
                        value: {
                            source: payload.source, duplicate, duplicateParts, invalid, invalidParts, dropped, droppedParts, queued, queuedParts, saved, savedParts, total, totalParts, processed, skipped,
                            start: payload.start,
                            skip: payload.skip,
                            end: payload.end
                        }
                    });
                    if(payload.campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER){
                        campaign = await this.campaignDao.getViberCampaign(payload.campaign.id, payload.campaign.accountId);
                    }else{
                        campaign = await this.campaignDao.getCampaign(payload.campaign.id, payload.campaign.accountId);
                    }
                    if (campaign.campaignStatusId !== CampaignStatus.LIVE) {
                        detectedPause = true;
                        transformStream.stop();
                        if (typeof (<any>readableStream).destroy === 'function')
                            (<any>readableStream).destroy();
                        this.logger.info({
                            message: 'Campaign process transaction stopped',
                            id: getFormattedCampaignIdForLogs(payload.campaign.id),
                            value: {
                                campaignStatusId: campaign.campaignStatusId,
                                reason: "Campaign status is not live"
                            }
                        });
                    }
                    cache.set(key, {active: true}, parseInt(process.env.CAMPAIGN_QUEUEING_CACHE_TTL));
                }, 1000);

                setTimeout(async () => {
                    var total = transformStream.getCount().total;
                    var processed = transformStream.getCount().processed;
                    var queued = queueStream.getCount().queued;
                    var saved = mongoStream.getCount().saved;
                    var totalParts = transformStream.getCount().totalParts;
                    var queuedParts = queueStream.getCount().queuedParts;
                    var savedParts = mongoStream.getCount().savedParts;
                    if (total === 0 && processed === 0 && queued === 0 && saved === 0) {
                        this.logger.error({
                            message: 'Campaign process transaction error',
                            id: getFormattedCampaignIdForLogs(payload.campaign.id),
                            value: {
                                source: payload.source, queued, queuedParts, saved, savedParts, total, totalParts, processed
                            }
                        });
                        process.exit(1);
                    }
                }, 120000);

                let alreadyExited = false;
            return await new Promise<CampaignTransactionSourceResultParams>(async (resolve, reject) => {
                let handleAllErrors = (name) => {
                    return async (error) => {
                        if (alreadyExited) return;
                        alreadyExited = true;

                        this.logger.error({
                            message: 'CAMPAIGN QUEUEING FAILED', id: getFormattedCampaignIdForLogs(payload.campaign.id),
                            reason: error.message,
                            value: {stack: error.stack, name, payload}
                        });

                        transformStream.stop();
                        // await this.campaignDao.updateCampaign(payload.campaign.id, payload.campaign.accountId, {
                        //     campaign_status_id: CampaignStatus.QUEUEING_FAILED,
                        // });
                        // await this.pt3CampaignBroadcastQ.publish(<any> {
                        //     type: payload.campaign.campaignTypeCode === CampaignTypeCodeEnum.EMAIL ? 'email' : 'sms',
                        //     campaignId: payload.campaign.id
                        // }, { delay: parseInt(process.env.PT3_CAMPAIGN_EVENT_HOOK_BROADCAST_INTERVAL) });

                        await this.campaignDao.updateCampaignTransactionSource({
                            id: payload.id,
                            accountId: payload.campaign.accountId,
                            campaignId: payload.campaign.id,
                            requestId: payload.requestId,
                            queued: queueStream.getCount().queued,
                            queuedParts: queueStream.getCount().queuedParts,
                            dropped: mongoStream.getCount().dropped,
                            droppedParts: mongoStream.getCount().droppedParts,
                            saved: mongoStream.getCount().saved,
                            savedParts: mongoStream.getCount().savedParts,
                            duplicate: mongoStream.getCount().duplicate,
                            duplicateParts: mongoStream.getCount().duplicateParts,
                            invalid: mongoStream.getCount().invalid,
                            invalidParts: mongoStream.getCount().invalidParts,
                            total: transformStream.getCount().total,
                            totalParts: transformStream.getCount().totalParts,
                            cost: transformStream.getCount().cost,
                            start: payload.start,
                            end: payload.end,
                            sourceType: payload.source.type,
                            fileId: payload.source.fileId,
                            contactIds: payload.source.contactIds,
                            contactGroupId: payload.source.contactGroupId,
                            completed: false
                        });

                        await this.pt3CampaignSlackUpdateQ.publish(<CampaignSlackUpdateJob> {
                            type: "child",
                            campaign: campaign,
                            text: `CAMPAIGN QUEUEING FAILED ${name}\nSource Type: ${payload.source.type}\n${error.message}\n${error.stack}\n${payload.source.type === CampaignSourceType.CONTACT? 'Contacts: ' +payload.source.contactIds.join(', ').substring(0, 15) + '...' : payload.source.type === CampaignSourceType.CONTACT_GROUP? 'ID: ' + payload.source.contactGroupId : 'ID: '+ payload.source.fileId}\n${payload.start||''}-${payload.end||''}`,
                            color:colors.DANGER
                        });
                        await cache.del(key);
                        clearInterval(interval);
                        reject(error);
                    };
                };

                    readableStream.on('error', handleAllErrors('CampaignTransactionSourceStream'));
                    transformStream.on('error', handleAllErrors('CampaignTransactionModelTransformationStream'));
                    mongoStream.on('error', handleAllErrors('CampaignTransactionDestinationStream'));
                    queueStream.on('error', handleAllErrors('JobQueueDestinationStream'));

                    let stream = readableStream
                        .pipe(transformStream)
                        .pipe(mongoStream)
                        .pipe(queueStream);
                    stream.on('finish', async () => {
                        try {
                            clearInterval(interval);

                            await cache.del(key);

                            let processTransactionSourceResult = {
                                id: payload.id,
                                accountId: payload.campaign.accountId,
                                campaignId: payload.campaign.id,
                                requestId: payload.requestId,
                                queued: queueStream.getCount().queued,
                                queuedParts: queueStream.getCount().queuedParts,
                                dropped: mongoStream.getCount().dropped,
                                droppedParts: mongoStream.getCount().droppedParts,
                                saved: mongoStream.getCount().saved,
                                savedParts: mongoStream.getCount().savedParts,
                                duplicate: mongoStream.getCount().duplicate,
                                duplicateParts: mongoStream.getCount().duplicateParts,
                                invalid: mongoStream.getCount().invalid,
                                invalidParts: mongoStream.getCount().invalidParts,
                                total: transformStream.getCount().total,
                                totalParts: transformStream.getCount().totalParts,
                                cost: queueStream.getCount().cost,
                                start: payload.start,
                                end: payload.end,
                                sourceType: payload.source.type,
                                fileId: payload.source.fileId,
                                contactIds: payload.source.contactIds,
                                contactGroupId: payload.source.contactGroupId,
                                completed: !detectedPause && !detectedShutdown
                            };

                            await this.campaignDao.updateCampaignTransactionSource(processTransactionSourceResult);
                            clearCachedCall(this.campaignDao.constructor.name, 'aggregateCampaignTransactionSource', [payload.campaign.id, payload.campaign.accountId]);

                            if (detectedShutdown) {
                                payload.skip = processTransactionSourceResult.total || 0;
                                let transactionSourceQ = this.getTransactionSourceQ(campaign.campaignTypeCode);

                                await transactionSourceQ.publish(payload, {
                                    priority: 1
                                });

                                this.logger.warn({
                                    message: 'processTransactionSource JOB Requeued',
                                    id: getFormattedCampaignIdForQeueuingLogs(payload.campaign.id),
                                    value: {
                                        payload
                                    }
                                });
                            } else {
                                if (!detectedPause && !detectedShutdown) {
                                    await this.pt3CampaignSlackUpdateQ.publish(<CampaignSlackUpdateJob> {
                                        type: "child",
                                        campaign: campaign,
                                        text: `Campaign source #${payload.id} finished\nPriority: ${payload.priority}\nSource Type: ${payload.source.type}
                                            ${payload.source.type === CampaignSourceType.CONTACT? 'Contacts: ' +payload.source.contactIds.join(', ').substring(0, 15) + '...' : payload.source.type === CampaignSourceType.CONTACT_GROUP? 'ID: ' + payload.source.contactGroupId : 'ID: '+ payload.source.fileId}
                                            ${payload.start||''}-${payload.end||''}
                                            ${JSON.stringify({queued: queueStream.getCount().queued,
                                            queuedParts: queueStream.getCount().queuedParts,
                                            dropped: mongoStream.getCount().dropped,
                                            droppedParts: mongoStream.getCount().droppedParts,
                                            invalid: mongoStream.getCount().invalid,
                                            invalidParts: mongoStream.getCount().invalidParts,
                                            duplicate: mongoStream.getCount().duplicate,
                                            duplicateParts: mongoStream.getCount().duplicateParts,
                                            saved: mongoStream.getCount().saved,
                                            savedParts: mongoStream.getCount().savedParts,
                                            total: transformStream.getCount().total,
                                            totalParts: transformStream.getCount().totalParts,
                                            cost: transformStream.getCount().cost,})}`,
                                        color: colors.SUCCESS
                                    });
                                    this.logger.info({
                                        message: 'Add to Campaign Slack Thread',
                                        id: getFormattedCampaignIdForQeueuingLogs(payload.campaign.id),
                                        value: {
                                            queued: queueStream.getCount().queued,
                                            queuedParts: queueStream.getCount().queuedParts,
                                            dropped: mongoStream.getCount().dropped,
                                            droppedParts: mongoStream.getCount().droppedParts,
                                            invalid: mongoStream.getCount().invalid,
                                            invalidParts: mongoStream.getCount().invalidParts,
                                            duplicate: mongoStream.getCount().duplicate,
                                            duplicateParts: mongoStream.getCount().duplicateParts,
                                            saved: mongoStream.getCount().saved,
                                            savedParts: mongoStream.getCount().savedParts,
                                            total: transformStream.getCount().total,
                                            totalParts: transformStream.getCount().totalParts,
                                            cost: transformStream.getCount().cost
                                        }
                                    })
                                    if (payload.source.localCSVPath) {
                                        let result = await this.s3Transport.deleteLocally(payload.source.localCSVPath);
                                        if (result)
                                            this.logger.info({
                                                message: 'Local file has been deleted',
                                                id: getFormattedCampaignIdForQeueuingLogs(payload.campaign.id),
                                                value: {
                                                    filePath: payload.source.localCSVPath
                                                }
                                            });
                                    }
                                }
                            }

                            let duration = new Date().getTime() - ts.getTime();
                            statCollector.add('campaign.sourceProcessing', {
                                processed: transformStream.getCount().processed,
                                tps: transformStream.getCount().processed / (duration / 1000),
                                queued: queueStream.getCount().queued,
                                queuedParts: queueStream.getCount().queuedParts,
                                dropped: mongoStream.getCount().dropped,
                                droppedParts: mongoStream.getCount().droppedParts,
                                saved: mongoStream.getCount().saved,
                                savedParts: mongoStream.getCount().savedParts,
                                duplicate: mongoStream.getCount().duplicate,
                                duplicateParts: mongoStream.getCount().duplicateParts,
                                invalid: mongoStream.getCount().invalid,
                                invalidParts: mongoStream.getCount().invalidParts,
                                total: transformStream.getCount().total,
                                totalParts: transformStream.getCount().totalParts,
                                cost: queueStream.getCount().cost,
                                duration: duration,
                                skipped: payload.skip,
                                waitingTime: transformStream.getTime(),
                                start: payload.start || undefined,
                                end: payload.end || undefined
                            }, {
                                accountId: payload.campaign.accountId,
                                campaignId: payload.campaign.id,
                                sourceType: payload.source.type
                            });
                            resolve(processTransactionSourceResult);
                        }
                        catch(e) {
                            reject(e);
                        }
                    })
                        .on('error', handleAllErrors('allStreams'));
                });
            } else {
                return null;
            }
        }

        async getCacheDriver() {
            return await CacheProvider.getProvider();
        }

        getCampaignTransactionDestinationStream(payload: CampaignSourceParams) {
            if (payload.campaign.campaignTypeCode === CampaignTypeCodeEnum.EMAIL) {
                return this.getEmailCampaignTransactionDestinationStream(payload);
            } else if (payload.campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER) {
                return this.getViberCampaignTransactionDestinationStream(payload);
            } else {
                return this.getSmsCampaignTransactionDestinationStream(payload);
            }
        }

        getJobQueueDestinationStream(jobQueue) {
            return new CampaignJobQueueDestinationStream(jobQueue);
        }

        getCampaignTransactionModelTransformationStream(payload: CampaignSourceParams) {
            return new CampaignTransactionModelTransformationStream(payload, this.broker);
        }

        getSmsCampaignTransactionDestinationStream(payload: CampaignSourceParams) {
            return new SmsCampaignTransactionDestinationStream(payload);
        }

        getViberCampaignTransactionDestinationStream(payload: CampaignSourceParams) {
            return new ViberCampaignTransactionDestinationStream(payload);
        }

        getEmailCampaignTransactionDestinationStream(payload: CampaignSourceParams) {
            return new EmailCampaignTransactionDestinationStream(payload);
        }

        getCampaignSourceStrategy(sourceType: CampaignSourceType) {
            return CampaignSourceStrategyFactory.createStrategy(sourceType);
        }

        getTransactionSourceQ(campaignTypeCode: CampaignTypeCodeEnum){
            if(campaignTypeCode === CampaignTypeCodeEnum.VIBER){
                return this.pt3ViberTransactionSourceQ;
            }else{
                return this.pt3CampaignTransactionSourceQ;
            }
        }

        async prepCampaign(params: PrepCampaignParams) {
            this.logger.info({ id: params.id, message: "PrepCampaignParams", params: params});
            let queueUrl = process.env.PT3_CAMPAIGN_TRANSACTION_Q.replace('id', '' + params.id);
            let jq = this.getJobQueue(queueUrl);
            await jq.init();
            // let createdSQSQueue = await this.createSqsQueue(params.id);
            let campaign = (params.transactionType === TransactionType.VIBER) ? await this.campaignDao.getViberCampaign(params.id, params.user.accountId) : await this.campaignDao.getCampaign(params.id, params.user.accountId);

            switch(params.transactionType){
                case 'email':{
                    params.transactionType = TransactionType.EMAIL
                }
                case 'sms':{
                    params.transactionType = TransactionType.SMS
                }
                case 'viber':{
                    params.transactionType = TransactionType.VIBER
                }
            }


            if(campaign && campaign.campaignStatusId === CampaignStatus.CANCELLED) {
                this.logger.warn({ id: getFormattedCampaignIdForLogs(params.id), message: `Invalid Campaign Status - ${campaign.campaignStatusCode}`});
                return;
            }
            // if (campaign && (campaign.campaignStatusId === CampaignStatus.QUEUE_WAIT || campaign.campaignStatusId === CampaignStatus.SCHEDULED)) {
                if(campaign.campaignStatusId === CampaignStatus.SCHEDULED) this.campaignDao.updateCampaign(params.id, params.user.accountId, { date_triggered: new Date() });

                await this.initializePt3CampaignStats(params.requestId, campaign);
                // Update campaign status = live
                await this.campaignDao.updateCampaign(params.id, params.user.accountId, {
                    campaign_status_id: CampaignStatus.LIVE
                });
                await this.pt3CampaignBroadcastQ.publish(<any> {
                    type: params.transactionType,
                    campaignId: params.id
                }, { delay: parseInt(process.env.PT3_CAMPAIGN_EVENT_HOOK_BROADCAST_INTERVAL) });
                await this.campaignDao.addCampaignLog({message: CampaignStatusLog.LIVE, createdById: campaign.userId, campaignId: campaign.id, isShown: 1});

                this.logger.info({ id: getFormattedCampaignIdForLogs(campaign.id), message: CampaignStatusLog.LIVE, createdById: campaign.userId, campaignId: campaign.id, isShown: 1});

                // Set cache campaign-queueing-[id] = true
                let cache = await CacheProvider.getProvider();
                let queueingCacheKey = process.env.PT3_CAMPAIGN_QUEUEING_CACHE_NAME.replace(/id$/, ''+params.id);

                await cache.set(queueingCacheKey, {active: true}, parseInt(process.env.CAMPAIGN_QUEUEING_CACHE_TTL));

                // let queueUrl = toSQSQueueUrl(createdSQSQueue.QueueUrl, '?concurrency=10&attempts=3&sendBatched=true&sendBatchSize=50');

                let job: CampaignLivePojo = {
                    campaignId: params.id,
                    accountId: params.user.accountId,
                    queueUrl: queueUrl,
                    mainJob: true,
                    // campaign traffic is secondary to API traffic, thus the low priority
                    smsPriorityLevel: params.accountSettings.smsPriorityLevel * 2
                };

                await this.pt3CampaignLiveQ.publish(job);

                let res = await this.sendToCampaignTransactionSourceQ(params, campaign, queueUrl);


                // let totalSourceCacheKey = process.env.PT3_CAMPAIGN_TOTAL_SOURCE_CACHE_NAME.replace(/id$/, ''+params.id);
                await this.campaignDao.updateCampaign(params.id,params.user.accountId,{total_transaction_source:res.totalSources})
                // await cache.set(totalSourceCacheKey, res, parseInt(process.env.CAMPAIGN_SOURCE_COUNT_CACHE_TTL));
                this.logger.info({ id: getFormattedCampaignIdForLogs(campaign.id), message: `Found ${res.totalSources} campaign sources`});

                if (res.totalSources === 0) {
                    await this.campaignDao.updateCampaign(params.id, params.user.accountId, {
                        campaign_status_id: CampaignStatus.NO_VALID_TRANSACTIONS
                    });
                    await this.campaignDao.addCampaignLog({message: CampaignStatusLog.NO_VALID_TRANSACTIONS, createdById: campaign.userId, campaignId: campaign.id, isShown: 0});

                    await this.pt3CampaignBroadcastQ.publish(<any> {
                        type: params.transactionType === TransactionType.EMAIL ? 'email' : 'sms',
                        campaignId: params.id
                    }, { delay: parseInt(process.env.PT3_CAMPAIGN_EVENT_HOOK_BROADCAST_INTERVAL) });
                }

                campaign = await this.campaignDao.getCampaign(params.id, params.user.accountId);
                await this.pt3CampaignSlackUpdateQ.publish(<CampaignSlackUpdateJob> {
                    type: 'parent',
                    campaign: campaign
                });
            // } else {
            //     this.logger.info({message: 'CAMPAIGN STATUS IS NOT QUEUED', id: getFormattedCampaignIdForLogs(campaign.id), value:{campaign, params}});
            //     throw new Error('Campaign status is not QUEUED');
            // }
        }

        // async deleteSqsQueue(id: any): Promise<any> {
        //     let qName = 'campaign-send-' + id + 'q';
        //     let QueueUrl = await this.sqs.getQueueUrl({
        //         QueueName: qName,
        //         QueueOwnerAWSAccountId: process.env.AWS_ACCOUNT_ID
        //     }).promise();
        //
        //     console.log('QueueUrl', QueueUrl);
        //
        // }

        async sendToCampaignTransactionSourceQ(params: PrepCampaignParams, campaign: Campaign, queueUrl: string): Promise<CampaignCompletionTracker> {
            let cache = await this.getCacheDriver();
            let currentPriority;
            if(campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER){
                currentPriority = parseInt(await cache.get(VIBER_CAMPAIGN_CURRENT_PRIORITY_CACHE_KEY)) || 0;
                statCollector.add('viberCampaign.sourcePriority', {
                    currentPriority: currentPriority
                });
            }else{
                currentPriority = parseInt(await cache.get(CAMPAIGN_CURRENT_PRIORITY_CACHE_KEY)) || 0;
                statCollector.add('campaign.sourcePriority', {
                    currentPriority: currentPriority
                });
            }

            /**
             * This is for bulk sms campaigns
             */
            let totalSources = 0;
            let totalRows = 0;
            if (campaign.campaignTypeCode === CampaignTypeCodeEnum.BULK || campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER)
                if(params.contactIds) {
                    let batch = _.chunk(params.contactIds, parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_CONTACT_BATCH_SIZE));
                    await Bluebird.each(batch, (async (contactIds) => {
                        for (let i = 1; i <= Math.ceil(contactIds.length / parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_CONTACT_BATCH_SIZE)); i++) {
                            let campaignSource = await this.campaignDao.getCampaignTransactionSource(campaign.id, campaign.accountId, totalSources);

                            if (!campaignSource || !campaignSource.completed) {
                                totalRows += parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_CONTACT_BATCH_SIZE);
                            }

                            let priority = currentPriority + (totalRows || 1);

                            let job: CampaignSourceParams = {
                                id: totalSources,
                                campaign: campaign,
                                queueUrl: queueUrl,
                                requestId: params.requestId,
                                accountSettings: params.accountSettings,
                                priority: priority,
                                skip: campaignSource && campaignSource.total || 0,
                                skipParts: campaignSource && campaignSource.totalParts || 0,
                                start: null,
                                end: null,
                                source: {
                                    type: CampaignSourceType.CONTACT,
                                    contactIds: contactIds
                                }
                            };
                            if (!campaignSource || !campaignSource.completed) {
                                let transactionSourceQ = this.getTransactionSourceQ(campaign.campaignTypeCode);

                                await transactionSourceQ.publish(job, {
                                    priority: priority
                                });

                                this.logger.info({
                                    message: 'Sent to campaign transaction source queue',
                                    id: getFormattedCampaignIdForLogs(campaign.id),
                                    value: {job, priority}
                                });
                            }
                            totalSources++;
                        }
                    }));
                }

            /**
             * This is for compose blast campaigns
             */
            if (campaign.campaignTypeCode === CampaignTypeCodeEnum.FILE_BLAST || campaign.campaignTypeCode === CampaignTypeCodeEnum.COMPOSE_BLAST || campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER || campaign.campaignTypeCode === CampaignTypeCodeEnum.BULK)
                if(params.fileIds) {
                await Bluebird.each(params.fileIds, (async (fileId) => {
                    let file = await this.fileDao.getFileDetails(fileId, campaign.accountId);
                    if(file.fileTypeCode !== FileTypeCodeEnum.COMPOSE_BLAST && file.fileTypeCode !== FileTypeCodeEnum.FILE_BLAST) return;
                    let csvFilePath = './uploads/campaign-' + campaign.id + '-file-' + fileId + '-' + file.filename.replace(/\.xls[x]?/, '.csv');

                    let chunk = 0;

                    let full_path = file.fullpath.split('.com/');
                    let filename = full_path[1];

                    await this.s3Transport.makeDirectorySafe('./uploads');

                    if(['xlsx', 'xls'].indexOf(file.filename.split('.').pop()) > -1) {
                        let temporaryFilename = this.getTemporaryFilename();

                        this.logger.info({
                            message: 'XLS/X file is uploaded!', id: getFormattedCampaignIdForQeueuingLogs(campaign.id), filename: file.filename } );

                        await this.s3Transport.saveToLocalFile(process.env.AWS_PT3_UPLOADS_BUCKET, filename, temporaryFilename, file.fileHash);
                        await this.s3Transport.convertXLSToCsv(temporaryFilename, csvFilePath);
                    } else {
                        this.logger.info({
                            message: 'CSV file is uploaded!',
                            id: getFormattedCampaignIdForQeueuingLogs(campaign.id),
                            filename: filename,
                            csvFilePath: csvFilePath,
                            fileHash: file.fileHash} );
                        try {
                            await this.s3Transport.saveToLocalFile(process.env.AWS_PT3_UPLOADS_BUCKET, filename, csvFilePath, file.fileHash);
                        }catch (e){
                            this.logger.error({message: 'saveToLocalFile FAILED',
                                reason: e.message,
                                value: {stack: e.stack, error: e},
                                id: getFormattedCampaignIdForQeueuingLogs(campaign.id)});
                            throw e;
                        }
                    }

                    let chunks = await this.s3Transport.chunkCsvFile(csvFilePath, parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_FILE_ROW_BATCH_SIZE));
                    await this.s3Transport.deleteLocally(csvFilePath);

                    for (let i = 1; i <= chunks; i++) {

                        let campaignSource = await this.campaignDao.getCampaignTransactionSource(campaign.id, campaign.accountId, totalSources);

                        if (!campaignSource || !campaignSource.completed) {
                            totalRows += parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_FILE_ROW_BATCH_SIZE);
                        }

                        let priority = currentPriority + (totalRows || 1);

                        let localChunkedCSVPath = csvFilePath + chunk++;
                        let job: CampaignSourceParams = {
                            id: totalSources,
                            campaign: campaign,
                            queueUrl: queueUrl,
                            requestId: params.requestId,
                            accountSettings: params.accountSettings,
                            priority: priority,
                            skip: campaignSource && campaignSource.total || 0,
                            skipParts: campaignSource && campaignSource.totalParts || 0,
                            start: (i-1) * parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_FILE_ROW_BATCH_SIZE),
                            end: i * parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_FILE_ROW_BATCH_SIZE),
                            source: {
                                type: CampaignSourceType.FILE,
                                localCSVPath: localChunkedCSVPath,
                                fileId: fileId
                            }
                        };

                        if (!campaignSource || !campaignSource.completed) {
                            let priority = job.priority;
                            let transactionSourceQ = this.getTransactionSourceQ(campaign.campaignTypeCode);

                            await transactionSourceQ.publish(job, {
                                priority: priority
                            });
                            this.logger.info({
                                message: 'Sent to campaign transaction source queue',
                                id: getFormattedCampaignIdForLogs(campaign.id),
                                value: {job: job, priority}
                            });
                        }
                        else {
                            await this.s3Transport.deleteLocally(localChunkedCSVPath);
                        }
                        totalSources++;
                    }

                }));
            }

            /**
             * This is for groupIds
             */
            if (campaign.campaignTypeCode === CampaignTypeCodeEnum.BULK || campaign.campaignTypeCode === CampaignTypeCodeEnum.EMAIL || campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER)
            if(params.groupIds) {
                await Bluebird.each(params.groupIds, (async (contactGroupId) => {
                    let contactGroup = await this.contactDao.getContactGroup(contactGroupId);
                    for (let i = 1; i <= Math.ceil(contactGroup.totalContacts / parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_CONTACT_GROUP_MEMBER_BATCH_SIZE)); i++) {
                        let campaignSource = await this.campaignDao.getCampaignTransactionSource(campaign.id, campaign.accountId, totalSources);

                        if (!campaignSource || !campaignSource.completed) {
                            totalRows += parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_CONTACT_GROUP_MEMBER_BATCH_SIZE);
                        }

                        let priority = currentPriority + (totalRows || 1);

                        let job: CampaignSourceParams = {
                            id: totalSources,
                            campaign: campaign,
                            queueUrl: queueUrl,
                            requestId: params.requestId,
                            accountSettings: params.accountSettings,
                            priority: priority,
                            skip: campaignSource && campaignSource.total || 0,
                            skipParts: campaignSource && campaignSource.totalParts || 0,
                            start: (i - 1) * parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_CONTACT_GROUP_MEMBER_BATCH_SIZE),
                            end: i * parseInt(process.env.PT3_CAMPAIGN_TRANSACTION_SOURCE_CONTACT_GROUP_MEMBER_BATCH_SIZE),
                            source: {
                                type: CampaignSourceType.CONTACT_GROUP,
                                contactGroupId: contactGroupId
                            }
                        };
                        if (!campaignSource || !campaignSource.completed) {
                            let transactionSourceQ = this.getTransactionSourceQ(campaign.campaignTypeCode);

                            await transactionSourceQ.publish(job, {
                                priority: priority
                            });

                            this.logger.info({
                                message: 'Sent to campaign transaction source queue',
                                id: getFormattedCampaignIdForLogs(campaign.id),
                                value: {job, priority}
                            });
                        }
                        totalSources++;
                    }
                }));
            }

            return {
                campaignId: campaign.id,
                totalSources: totalSources
            }
        }

        async recomputeViberCampaignTotalCost(params: any) {
            try {
                let campaign: Campaign = await this.campaignDao.getCampaign(params.campaignId, params.accountId);
                if(campaign.campaignTypeCode !== CampaignTypeCodeEnum.VIBER) return;

                // todo count number of queued
                let viberTransactionsCountQueuedParams = {
                    categoryId: params.campaignId,
                    accountId: params.accountId,
                    status: ''
                };
                let aggregatedQueued = await this.campaignDao.getViberQueuedTransactionsCount(viberTransactionsCountQueuedParams, campaign.isTestCampaign);
                this.logger.info({ message: 'got aggregatedQueued', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedQueued }});

                // todo count number of sent
                let viberTransactionsCountSentParams = {
                    categoryId: params.campaignId,
                    accountId: params.accountId,
                    status: 'sent'
                }

                let aggregatedSent = await this.campaignDao.getViberSentTransactionsCount(viberTransactionsCountSentParams);
                this.logger.info({ message: 'got aggregatedSent', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedSent }});
                // todo count number of delivered (viber)
                let viberTransactionsCountDeliveredParams  = {
                    categoryId: params.campaignId,
                    accountId: params.accountId,
                    status: 'delivered'
                };
                let aggregatedDelivered = await this.campaignDao.getViberDlrCount(viberTransactionsCountDeliveredParams);
                this.logger.info({ message: 'got aggregatedDelivered', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedDelivered }});

                // todo count number of bounced transactions
                let viberTransactionsCountBouncedParams = {
                    categoryId: params.campaignId,
                    accountId: params.accountId,
                    status: 'bounced'
                };
                let aggregatedBounced = await this.campaignDao.getViberDlrCount(viberTransactionsCountBouncedParams);
                this.logger.info({ message: 'got aggregatedBounced', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedBounced }});

                let viberTransactionCountRejectedParams = {
                    categoryId: params.campaignId,
                    accountId: params.accountId,
                    status: 'rejected'
                }
                let aggregatedRejected = await this.campaignDao.getViberDlrCount(viberTransactionCountRejectedParams);
                this.logger.info({ message: 'got aggregatedRejected', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedRejected }});

                let viberTransactionCountRejectedCountryParams = {
                    categoryId: params.campaignId,
                    accountId: params.accountId,
                    status: 'rejectedcountry'
                }
                let aggregatedRejectedCountry = await this.campaignDao.getViberDlrCount(viberTransactionCountRejectedCountryParams);
                this.logger.info({ message: 'got aggregatedRejectedCountry', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedRejectedCountry }});

                // todo count number of clicked
                let aggregatedClick = await this.viberDao.countCampaignClickedStats(params.campaignId);
                this.logger.info({ message: 'got aggregatedClick', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedClick, 'campaignId': params.campaignId, 'time': new Date() }});

                // todo count number of seen
                let aggregatedSeen = await this.viberDao.countCampaignSeenStats(params.campaignId.toString(), params.accountId);
                this.logger.info({ message: 'got aggregatedSeen', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedSeen }});

                let aggregatedFallbackSent = 0;
                let aggregatedFallbackDelivered = 0;
                let aggregatedFallbackBounced = 0;
                let aggregatedFallbackExpired = 0;
                if(campaign.isFallback) {
                    // todo count number of fallback sent transactions
                    let fallbackTransactionsCountSentParams = {
                        categoryId: params.campaignId,
                        accountId: params.accountId,
                        status: 'sent'
                    }
                    aggregatedFallbackSent = await this.campaignDao.getSmsSentTransactionsCount(fallbackTransactionsCountSentParams, campaign.isTestCampaign);
                    this.logger.info({ message: 'got aggregatedFallbackSent', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedFallbackSent }});

                    // todo count number of fallback delivered transactions
                    let fallbackTransactionsCountDeliveredParams = {
                        categoryId: params.campaignId,
                        accountId: params.accountId,
                        status: 'delivered'
                    }
                    aggregatedFallbackDelivered = await this.campaignDao.getFallbackTransactionsCount(fallbackTransactionsCountDeliveredParams);
                    this.logger.info({ message: 'got aggregatedFallbackDelivered', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedFallbackDelivered }});

                    // todo count number of fallback bounced transactions
                    let fallbackTransactionCountBouncedParams = {
                        categoryId: params.campaignId,
                        accountId: params.accountId,
                        status: 'bounced'
                    }
                    aggregatedFallbackBounced = await this.campaignDao.getFallbackTransactionsCount(fallbackTransactionCountBouncedParams);
                    this.logger.info({ message: 'got aggregatedFallbackBounced', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedFallbackBounced }});

                    let fallbackTransactionCountExpiredParams = {
                        categoryId: params.campaignId,
                        accountId: params.accountId,
                        status: 'expired'
                    }
                    aggregatedFallbackExpired = await this.campaignDao.getFallbackTransactionsCount(fallbackTransactionCountExpiredParams);
                    this.logger.info({ message: 'got aggregatedFallbackExpired', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedFallbackBounced }});
                }

                // todo count number of error
                let aggregatedError = await this.campaignDao.aggregateError(params.campaignId.toString(), params.accountId);
                let aggregateFallbackError = await this.campaignDao.aggregateFallbackError(params.campaignId.toString(), params.accountId);
                this.logger.info({ message: 'got aggregatedError', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedError }});

                let aggregatedTransactionSource = await this.campaignDao.aggregateCampaignTransactionSource(params.campaignId, params.accountId);
                this.logger.info({ message: 'got aggregatedDropped', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { aggregatedTransactionSource }});

                let finalSent;
                let finalQueued;
                let finalBounced;
                let finalDelivered = aggregatedDelivered + aggregatedFallbackDelivered;
                if(campaign.isFallback) { // if the campaign has a fallback
                    finalSent = (aggregatedSent + aggregatedFallbackSent) - aggregateFallbackError;
                    if(campaign.isTestCampaign) {
                        finalQueued = aggregatedQueued + aggregatedFallbackSent
                        finalBounced = aggregatedBounced + aggregatedRejected + aggregatedRejectedCountry + aggregatedFallbackBounced
                    } else {
                        finalQueued = aggregatedQueued + aggregatedFallbackSent;
                        finalBounced = aggregatedFallbackBounced + aggregatedRejectedCountry + aggregatedFallbackExpired;
                    }
                } else { // if the campaign has NO fallback
                    finalSent = aggregatedSent - aggregateFallbackError;
                    finalQueued = aggregatedQueued;
                    finalBounced = aggregatedBounced + aggregatedRejected + aggregatedRejectedCountry;
                }

                await this.campaignStatsDao.updateCampaignStatsRecompute(params.campaignId, {
                    bounced: finalBounced,
                    delivered: finalDelivered,
                    sent: finalSent,
                    queued: finalQueued,
                    clicked: aggregatedClick,
                    // dropped: aggregatedTransactionSource.dropped,
                    dropped: aggregatedError + aggregatedTransactionSource.duplicateParts,
                    errors: aggregatedError
                });
                this.logger.info('after update campaign stats', aggregatedClick)
                await this.campaignStatsDao.updateViberCampaignStatsRecompute(params.campaignId, {
                    fallback_delivered: aggregatedFallbackDelivered,
                    seen: aggregatedSeen,
                    viber_delivered: aggregatedDelivered
                });

                let campaignCost = await this.accountDao.getTotalBillingTransaction(campaign)
                this.logger.info({ message: 'got campaignCost', id: getFormattedCampaignIdForStatsLogs(params.campaignId), value: { campaignCost }});

                await this.campaignDao.updateCampaign(params.campaignId, params.accountId, {
                    campaign_status_id: CampaignStatus.COMPLETED,
                    date_completed: params.updateCampaignParams.date_completed,
                    campaign_cost: campaignCost,
                    total_recipients: aggregatedTransactionSource.total - (aggregatedError + aggregatedTransactionSource.duplicateParts)
                });

                if(moment() <= moment(params.updateCampaignParams.date_completed).add(process.env.RECOMPUTE_MAX_DELAY_IN_DAYS, 'days')) {
                    await this.recomputeTotalCostQ.publish(<any>{
                        campaignId: params.campaignId,
                        accountId: params.accountId,
                        updateCampaignParams: {
                            campaign_status_id: CampaignStatus.COMPLETED,
                            date_completed: params.updateCampaignParams.date_completed,
                            total_recipients: aggregatedTransactionSource.total - (aggregatedError + aggregatedTransactionSource.duplicateParts)
                        }
                    }, {
                        delay: parseInt(process.env.RECOMPUTE_INTERVAL_AFTER_FIRST_RECOMPUTE)
                    });
                } else {
                    this.logger.info({ message: `recomputeCampaignTotalCost run for ${process.env.RECOMPUTE_MAX_DELAY_IN_DAYS} days`, id: `recompute-campaign-${ params.campaignId }` });
                }
            } catch(e) {
                this.logger.error({message: 'recomputeCampaignTotalCost FAILED', reason: e.message, value: e, id: getFormattedCampaignIdForStatsLogs(params.campaignId)});
                throw e;
            }
        }

        async sendCampaignTransactions(payload: CampaignLivePojo) {
            let campaign: Campaign = await this.campaignDao.getCampaign(payload.campaignId, payload.accountId);
            let cache = await this.getCacheDriver();

            await cache.set(`${payload.campaignId}-mainJob`, true, parseInt(process.env.MAIN_JOB_CACHE_TTL));

            if (payload.mainJob === true)
                if (process.env.PT3_CAMPAIGN_LIVE_HELPER_JOBS && isFinite(parseInt(process.env.PT3_CAMPAIGN_LIVE_HELPER_JOBS))) {

                    for (let i = 1; i <= parseInt(process.env.PT3_CAMPAIGN_LIVE_HELPER_JOBS); i++) {

                        let helperJob: CampaignLivePojo = {
                            campaignId: payload.campaignId,
                            accountId: payload.accountId,
                            queueUrl: payload.queueUrl,
                            mainJob: false,
                            helperId: i,
                            // campaign traffic is secondary to API traffic, thus the low priority
                            smsPriorityLevel: payload.smsPriorityLevel
                        };

                        await this.pt3CampaignLiveQ.publish(helperJob);
                    }
                }

            let jq: JobQueue = this.getJobQueue(payload.queueUrl);
            // await this.reportCampaignJobCount(jq, campaign);
            if (campaign.campaignStatusId === CampaignStatus.LIVE) {
                if (campaign.campaignTypeCode === CampaignTypeCodeEnum.EMAIL) {
                    jq.listen(async (job: TypedJob<EmailCampaignTransactionRequest>) => {
                        this.logger.info({message: 'Got EmailCampaignTransactionRequest', id: getFormattedCampaignIdForSendingLogs(job.data.transactionId)});
                        await this.sendEmailTransactionRequest(job.data);
                    });
                }else if(campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER){
                    jq.listen(async (job: TypedJob<ViberCampaignTransactionRequest>) => {
                        this.logger.info({message: 'Got ViberCampaignTransactionRequest', value: {transactionId: job.data.transactionId}, id: getFormattedCampaignIdForSendingLogs(payload.campaignId)});
                        await this.sendViberTransactionRequest(job.data, payload.smsPriorityLevel);
                    });
                }
                else {
                    jq.listen(async (job: TypedJob<SMSCampaignTransactionRequest>) => {
                        this.logger.info({message: 'Got SMSCampaignTransactionRequest', id: getFormattedCampaignIdForSendingLogs(job.data.transactionId)});
                        await this.sendSMSTransactionRequest(job.data, payload.smsPriorityLevel)
                    });
                }

                return await new Promise((resolve, reject) => {
                    setTimeout(async () => {
                        try {

                            let start = new Date();
                            statCollector.add('jq.stop', {value: 1},{name:"jt.stop"}, start);

                            await jq.stop();

                            let end = new Date();
                            var seconds = (end.getTime() - start.getTime()) / 1000;
                            statCollector.add('jq.stop', {value: seconds},{name:"jt.stop"}, end);

                            start = new Date();
                            statCollector.add('campaignStillSending', {value: 1},{name:"campaignStillSending"}, start);

                            let campaignStillSending = await this.campaignStillSending(jq, payload);

                            end = new Date();
                            var seconds = (end.getTime() - start.getTime()) / 1000;
                            statCollector.add('campaignStillSending', {value: seconds},{name:"campaignStillSending"}, end);

                            // CAMPAIGN SOURCES ARE STILL BEING QUEUED
                            if (campaignStillSending) {
                                this.logger.info({
                                    message: "Campaign still queueing",
                                    id: getFormattedCampaignIdForSendingLogs(payload.campaignId),
                                    value: {
                                        payload: payload
                                    }
                                });
                                await Bluebird.delay(parseInt(process.env.CAMPAIGN_LIVE_REQUEUEING_DELAY));

                                if (payload.mainJob === true) {
                                    await this.pt3CampaignLiveQ.publish(payload);
                                    this.logger.info({
                                        message: "Main job requeued",
                                        value: payload,
                                        id: getFormattedCampaignIdForSendingLogs(payload.campaignId)
                                    });
                                }
                            }
                            // ALL CAMPAIGN SOURCES HAVE BEEN QUEUED AND NO MORE TRANSACTIONS LEFT TO SEND
                            else if (payload.mainJob === true) {

                                start = new Date();
                                statCollector.add('jq.remove', {value: 1},{name:"jq.remove"}, start);

                                await jq.remove();

                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('jq.remove', {value: seconds},{name:"jq.remove"}, end);

                                start = new Date();
                                statCollector.add('deductAccountWithholding', {value: 1},{name:"deductAccountWithholding"}, start);

                                await this.accountDao.deductAccountWithholding(campaign.accountId, campaign.costEstimate);

                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('deductAccountWithholding', {value: seconds},{name:"deductAccountWithholding"}, end);

                                this.logger.info({id: campaign.id, message: CampaignStatusLog.DEDUCT_WITHHOLDING + campaign.costEstimate});
                                await this.campaignDao.addCampaignLog({
                                    message: CampaignStatusLog.DEDUCT_WITHHOLDING + " " + campaign.costEstimate,
                                    createdById: campaign.userId,
                                    campaignId: campaign.id,
                                    isShown: 0
                                });

                                start = new Date();
                                statCollector.add('addCampaignLog', {value: 1},{name:"addCampaignLog"}, start);

                                await this.campaignDao.addCampaignLog({
                                    message: CampaignStatusLog.DEDUCT_WITHHOLDING,
                                    createdById: campaign.userId,
                                    campaignId: campaign.id,
                                    isShown: 0
                                });

                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('addCampaignLog', {value: seconds},{name:"addCampaignLog"}, start);

                                start = new Date();
                                statCollector.add('aggregateCampaignTransactionSource', {value: 1},{name:"aggregateCampaignTransactionSource"}, start);

                                let aggregatedCampaignTransactionSource = await this.campaignDao.aggregateCampaignTransactionSource(payload.campaignId, payload.accountId);

                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('aggregateCampaignTransactionSource', {value: seconds},{name:"aggregateCampaignTransactionSource"}, start);

                                if (campaign.campaignTypeCode !== CampaignTypeCodeEnum.VIBER){
                                    let params = {
                                        campaignId: payload.campaignId,
                                        queued: aggregatedCampaignTransactionSource.queuedParts
                                    };
                                    start = new Date();
                                    statCollector.add('updateCampaignStats', {value: 1},{name:"updateCampaignStats"}, start);
                                    await this.campaignStatsDao.updateCampaignStats(params);
                                    end = new Date();
                                    var seconds = (end.getTime() - start.getTime()) / 1000;
                                    statCollector.add('updateCampaignStats', {value: seconds},{name:"updateCampaignStats"}, start);
                                }

                                start = new Date();
                                statCollector.add('incrementCampaignStats', {value: 1},{name:"incrementCampaignStats"}, start);
                                await this.campaignStatsDao.incrementCampaignStats(payload.campaignId, {dropped: aggregatedCampaignTransactionSource.dropped});
                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('incrementCampaignStats', {value: seconds},{name:"incrementCampaignStats"}, start);

                                await this.pt3CampaignBroadcastQ.publish(<any>{
                                    type: campaign.campaignTypeCode === CampaignTypeCodeEnum.EMAIL ? 'email' : 'sms',
                                    campaignId: campaign.id
                                }, {delay: parseInt(process.env.PT3_CAMPAIGN_EVENT_HOOK_BROADCAST_INTERVAL)});

                                let dateCompleted = new Date();
                                let total_recipients =  aggregatedCampaignTransactionSource.total - aggregatedCampaignTransactionSource.dropped;

                                start = new Date();

                                await this.campaignDao.updateCampaign(payload.campaignId, payload.accountId, {
                                    campaign_status_id: CampaignStatus.COMPLETED,
                                    date_completed: dateCompleted,
                                    campaign_cost: aggregatedCampaignTransactionSource.cost,
                                    total_recipients: total_recipients
                                });

                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('updateCampaign', {value: seconds},{name:"updateCampaign"}, start);

                                let convertedTtl;
                                if(campaign.unit === 'mins') convertedTtl = campaign.ttl * 60 * 1000;
                                else if(campaign.unit === 'hrs') convertedTtl = campaign.ttl * 3600 * 1000;

                                if(campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER){
                                    // push to a new queue for proper handling
                                    await this.recomputeTotalCostQ.publish(<any>{
                                        campaignId: payload.campaignId,
                                        accountId: payload.accountId,
                                        updateCampaignParams: {
                                            campaign_status_id: CampaignStatus.COMPLETED,
                                            date_completed: dateCompleted,
                                            total_recipients: aggregatedCampaignTransactionSource.total
                                        }
                                    }, { delay: (campaign.ttl ? parseInt(convertedTtl) + parseInt(process.env.RECOMPUTE_DELAY) : parseInt(process.env.RECOMPUTE_DELAY))});

                                    this.logger.info({ message: 'pushed recomputeTotalCostQ job', id: getFormattedCampaignIdForLogs(payload.campaignId) });
                                }
                                await this.pt3CampaignBroadcastQ.publish(<any>{
                                    type: campaign.campaignTypeCode === CampaignTypeCodeEnum.EMAIL ? 'email' : 'sms',
                                    campaignId: campaign.id
                                }, {delay: parseInt(process.env.PT3_CAMPAIGN_EVENT_HOOK_BROADCAST_INTERVAL)});

                                let duration = (dateCompleted.getTime() - campaign.dateTriggered.getTime()) / 1000;
                                statCollector.add('campaign.completed', {
                                    duration: duration,
                                    count: aggregatedCampaignTransactionSource.queued,
                                    tps: aggregatedCampaignTransactionSource.queued / duration
                                });

                                this.logger.info({
                                    message: 'CAMPAIGN COMPLETED',
                                    value: aggregatedCampaignTransactionSource,
                                    id: getFormattedCampaignIdForLogs(payload.campaignId)
                                });

                                start = new Date();
                                await this.campaignDao.addCampaignLog({
                                    message: CampaignStatusLog.COMPLETED,
                                    createdById: campaign.userId,
                                    campaignId: campaign.id,
                                    isShown: 1
                                });
                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('addCampaignLog', {value: seconds},{name:"addCampaignLog"}, start);
                                campaign.campaignStatusId = CampaignStatus.COMPLETED;
                                campaign.campaignStatusCode = 'completed';
                                campaign.dateCompleted = dateCompleted;
                                await this.pt3CampaignSlackUpdateQ.publish(<CampaignSlackUpdateJob> {
                                    type: 'parent',
                                    campaign: campaign
                                });
                                start = new Date();
                                let user = await this.accountDao.getUser({
                                    user_id: campaign.userId,
                                    account_id: campaign.accountId
                                });
                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('getUser', {value: seconds},{name:"getUser"}, start);
                                if (campaign.fileIds && campaign.fileIds.length > 0)

                                    await Promise.all(campaign.fileIds.map(async (fileId) => {
                                        start = new Date();
                                        let file = await this.fileDao.getFileDetails(fileId, campaign.accountId);
                                        end = new Date();
                                        var seconds = (end.getTime() - start.getTime()) / 1000;
                                        statCollector.add('getFileDetails', {value: seconds},{name:"getFileDetails"}, start);
                                        let csvFilePath = './uploads/campaign-' + campaign.id + '-file-' + fileId + '-' + file.filename.replace(/\.xls[x]?/, '.csv');
                                        start = new Date();
                                        await this.s3Transport.deleteLocally(csvFilePath);
                                        end = new Date();
                                        var seconds = (end.getTime() - start.getTime()) / 1000;
                                        statCollector.add('deleteLocally', {value: seconds},{name:"deleteLocally"}, start);
                                    }));

                                let subject = "Campaign Completed";
                                start = new Date();
                                let timezone = await this.accountDao.getAccountTimezone(campaign.accountId);
                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('getAccountTimezone', {value: seconds},{name:"getAccountTimezone"}, start);
                                let emailNotification = {
                                    reportId: campaign.id,
                                    emailAddress: user.user_email,
                                    name: user.user_first_name + ' ' + user.user_last_name,
                                    subject: subject,
                                    type: EmailNotificationEnum.CAMPAIGNCOMPLETED,
                                    fromEmail: process.env.FROM_EMAIL,
                                    fromName: process.env.FROM_NAME,
                                    campaignType: campaign.campaignTypeCode === 'email' ? 'email' : 'sms',
                                    campaignName: campaign.name,
                                    campaignDateCompleted: moment(dateCompleted).tz(timezone.name).format('DD MMM YYYY [at] hh:mm a')
                                };
                                // await this.reportCampaignJobCount(jq, campaign);
                                start = new Date();
                                if(!campaign.isTestCampaign)
                                await this.broker.call('v1.pt3email.notify', emailNotification);
                                end = new Date();
                                var seconds = (end.getTime() - start.getTime()) / 1000;
                                statCollector.add('emailNotification', {value: seconds},{name:"emailNotification"}, start);
                            } else {
                                this.logger.info({
                                    message: "Campaign Live Helper Job is done",
                                    id: getFormattedCampaignIdForSendingLogs(payload.campaignId),
                                    value: {helperId: payload.helperId}
                                });
                            }
                            resolve();
                        } catch (e) {
                            reject(e);
                        }

                    }, parseInt(process.env.PT3_CAMPAIGN_MAX_RUNTIME));
                });

            } else {
                if (payload.mainJob === true) {
                    await this.pt3CampaignSlackUpdateQ.publish(<CampaignSlackUpdateJob> {
                        type: 'parent',
                        campaign: campaign
                    });
                }
                this.logger.info({message: 'Draining campaign jobs', id: getFormattedCampaignIdForSendingLogs(payload.campaignId), value: {campaignStatusId: campaign.campaignStatusId}});
                return await new Promise(async (resolve, reject) => {
                    if (campaign.campaignTypeCode === CampaignTypeCodeEnum.EMAIL) {
                        jq.listen(async (job: TypedJob<EmailCampaignTransactionRequest>) => {
                            this.logger.info({message: 'Got EmailCampaignTransactionRequest for draining', id: job.data.transactionId});
                            await this.sendEmailTransactionRequest(job.data);

                        });
                    } else if(campaign.campaignTypeCode === CampaignTypeCodeEnum.VIBER) {
                        jq.listen(async (job: TypedJob<ViberCampaignTransactionRequest>) => {
                            this.logger.info({ message: 'Got ViberCampaignTransactionRequest for draining', id: job.data.transactionId });
                            await this.sendViberTransactionRequest(job.data, payload.smsPriorityLevel);

                        });
                    } else {
                        jq.listen(async (job: TypedJob<SMSCampaignTransactionRequest>) => {
                            this.logger.info({message: 'Got SMSCampaignTransactionRequest for draining', id: job.data.transactionId});
                            await this.sendSMSTransactionRequest(job.data, payload.smsPriorityLevel);

                        });
                    }

                    while(await this.campaignStillQueueing(jq, payload)) {
                        this.logger.info({message: 'Waiting for campaign to stop queueing', id: getFormattedCampaignIdForSendingLogs(payload.campaignId)});
                        await Bluebird.delay(1000);
                        await this.reportCampaignJobCount(jq, campaign);
                    }

                    jq.stop();
                    resolve('drained');
                });
            }
        }

        async reportCampaignJobCount(jq: JobQueue, campaign: Campaign) {
            let res = await jq.getJobCount();
            statCollector.add('jobqueue.length', {
                waiting: res.waiting,
                active: res.active,
                completed: res.completed,
                failed: res.failed,
                delayed: res.delayed,
                paused: res.paused
            }, {
                queue: jq.options.queueId
            });
        }

        getJobQueue(queueUrl: string): JobQueue {
            return JobQueueFactory.createJobQueue(parseQueueOptions(queueUrl));
        }

        async campaignStillSending(transactionJobQueue: JobQueue, payload: CampaignLivePojo) {
            let isTransactionJobQueueEmpty = await transactionJobQueue.isEmpty();
            let isTransactionJobQueueNotEmpty = isTransactionJobQueueEmpty === false;

            if (isTransactionJobQueueNotEmpty) {
                this.logger.info({
                    message: "Campaign still sending, job queue not empty", id: getFormattedCampaignIdForSendingLogs(payload.campaignId)
                });
                return isTransactionJobQueueNotEmpty;
            }

            let cache = await CacheProvider.getProvider();

            let queueingCacheKey = process.env.PT3_CAMPAIGN_QUEUEING_CACHE_NAME.replace(/id$/, '' + payload.campaignId);
            let campaignQueueingFlag = await cache.get(queueingCacheKey);
            let isACampaignSourceStillBeingRead = (campaignQueueingFlag && campaignQueueingFlag.active === true);

            if (isACampaignSourceStillBeingRead) {
                this.logger.info({
                    message: "Campaign still sending, source still being read", id: getFormattedCampaignIdForSendingLogs(payload.campaignId)
                });
                return isACampaignSourceStillBeingRead;
            }

            // let totalSourceCacheKey = process.env.PT3_CAMPAIGN_TOTAL_SOURCE_CACHE_NAME.replace(/id$/, '' + payload.campaignId);
            let campaignCompletionTracker: CampaignCompletionTracker = await this.campaignDao.getCampaignTotalTransactionSource(payload.campaignId,payload.accountId)

            let aggregatedCampaignTransactionSource = await this.campaignDao.aggregateCampaignTransactionSource(payload.campaignId, payload.accountId);
            let allSourcesNotYetDone = !campaignCompletionTracker || !aggregatedCampaignTransactionSource || (campaignCompletionTracker.totalSources !== aggregatedCampaignTransactionSource.count);
            if (allSourcesNotYetDone) {
                this.logger.info({
                    message: campaignCompletionTracker ? "Campaign still sending, sources not yet read" : "Campaign still sending, totalSources not cached",
                    id: getFormattedCampaignIdForSendingLogs(payload.campaignId),
                    value: {
                        remaining: (campaignCompletionTracker && campaignCompletionTracker.totalSources || 0) - (aggregatedCampaignTransactionSource && aggregatedCampaignTransactionSource.count || 0),
                        total: (campaignCompletionTracker && campaignCompletionTracker.totalSources || 0),
                        done: (aggregatedCampaignTransactionSource && aggregatedCampaignTransactionSource.count || 0),
                        campaignCompletionTracker: campaignCompletionTracker,
                        aggregatedCampaignTransactionSource: aggregatedCampaignTransactionSource

                    }
                });
                return allSourcesNotYetDone;
            }

            return isTransactionJobQueueNotEmpty || isACampaignSourceStillBeingRead || allSourcesNotYetDone;
        }

        async campaignStillQueueing(transactionJobQueue: JobQueue, payload: CampaignLivePojo) {
            let isTransactionJobQueueEmpty = await transactionJobQueue.isEmpty();
            let isTransactionJobQueueNotEmpty = isTransactionJobQueueEmpty === false;

            if (isTransactionJobQueueNotEmpty) {
                this.logger.info({
                    message: "Campaign still sending, job queue not empty", id: getFormattedCampaignIdForSendingLogs(payload.campaignId)
                });
                return isTransactionJobQueueNotEmpty;
            }

            let cache = await CacheProvider.getProvider();

            let queueingCacheKey = process.env.PT3_CAMPAIGN_QUEUEING_CACHE_NAME.replace(/id$/, '' + payload.campaignId);
            let campaignQueueingFlag = await cache.get(queueingCacheKey);
            let isACampaignSourceStillBeingRead = (campaignQueueingFlag && campaignQueueingFlag.active === true);

            if (isACampaignSourceStillBeingRead) {
                this.logger.info({
                    message: "Campaign still sending, source still being read", id: getFormattedCampaignIdForSendingLogs(payload.campaignId)
                });
                return isACampaignSourceStillBeingRead;
            }

            return isTransactionJobQueueNotEmpty || isACampaignSourceStillBeingRead;
        }

        async sendSMSTransactionRequest(transaction: SMSCampaignTransactionRequest, priorityLevel: number) {
            if (transaction) {

                let sha = crypto.createHash('sha1');
                let hash = sha.update(transaction.transactionId + transaction.to + process.env.SALT_FOR_UNSUBSCRIBE);
                let json = {
                    id: transaction.transactionId,
                    key: sha.digest('hex'),
                    mobile_number: transaction.to,
                    account_id: transaction.accountId,
                    sender_id: transaction.from,
                    campaign_id: transaction.categoryId,
                    version: 'orion'
                };
                let ts = new Date();

                if((transaction.text.indexOf(OLD_OPT_OUT_LINK) !== -1)) { /* old links*/

                    let mongodbDoc = await short.generate({
                        URL: JSON.stringify(json)
                    });

                    this.logger.info({
                        id: transaction.transactionId,
                        message: "Sent to MDBX",
                        value: {json: json, hash: mongodbDoc.hash}
                    });
                    if(mongodbDoc.hash !== ''){
                        transaction.text = transaction.text.replace(OLD_OPT_OUT_LINK, process.env.MDBX_URL +  mongodbDoc.hash + ' ');
                    }
                }

                if((transaction.text.indexOf(NEW_OPT_OUT_LINK) !== -1) ) /* new links*/
                {
                    transaction.text = await this.linkGenerator.generateOptOutLink(transaction.text, json);
                    //get the value of nanoid hash from text
                    let baseLinkLength = process.env.MDBX_URL.length
                    let indexOfBaseLink = transaction.text.indexOf(process.env.MDBX_URL )
                    let lastIndexOfBaseLink = indexOfBaseLink + baseLinkLength;
                    let nanoHash = transaction.text.substring(lastIndexOfBaseLink, lastIndexOfBaseLink + 8);


                    this.logger.info({
                        id: transaction.transactionId,
                        message: "Sent to MDBX",
                        value: {json: json, hash: nanoHash}
                    });

                }

                let p4: SmsPojo = {
                    accountId: transaction.accountId,
                    userId: transaction.userId,
                    from: transaction.from,
                    to: transaction.to,
                    body: transaction.text,
                    transactionId: transaction.transactionId,
                    requestId: transaction.requestId,
                    transactionChannelType: transaction.transactionChannelType,
                    categoryId: transaction.categoryId,
                    dlrEventHookType: DlrEventHookType.PT3_SMS_CAMPAIGN,
                    sendingEventHookType: SendingEventHookType.PT3_SMS_CAMPAIGN,
                    priorityLevel: priorityLevel,
                    isTestTransaction: transaction.isTestTransaction
                };

                try {

                    let result = await this.broker.call("v2.sms.create", p4);
                    let duration = new Date().getTime() - ts.getTime();

                    this.logger.info({
                        id: getFormattedCampaignIdForSendingLogs(transaction.categoryId),
                        message: "Sent to SMS Service",
                        value: {transactionId: transaction.transactionId, to: transaction.to}
                    });

                    /**
                     *
                     *  report to influxdb
                     *
                     * */
                    statCollector.add('smscampaign.sent', {
                        count: 1,
                        duration: duration
                    }, {
                        accountId: transaction.accountId,
                        campaignId: transaction.categoryId
                    });

                    if (!result) {
                        statCollector.addOrionHealth(ServiceCode.PT3SMS_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.error, duration);
                        throw new InvalidSendSMSRequestError("orion-viber");
                    } else {
                        // Duration of call to sms service is above 100ms
                        if (duration >= 100)
                            statCollector.addOrionHealth(ServiceCode.PT3SMS_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.warning, duration);
                        else
                            statCollector.addOrionHealth(ServiceCode.PT3SMS_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.success, duration);
                    }

                    return result;
                }
                catch (e) {
                    let duration = new Date().getTime() - ts.getTime();
                    statCollector.addOrionHealth(ServiceCode.PT3SMS_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.error, duration);

                    this.logger.error({
                        id: transaction.transactionId,
                        message: "CAMPAIGN TRANSACTION SENDING FAILED",
                        reason: e.message,
                        value: {stack: e.stack}
                    });

                    this.logger.error({id: transaction.transactionId, message: 'Error Sending Request' , reason: e.message, value: {stack: e.stack, transactionId: transaction.transactionId}});

                }
            }
            else {
                return null;
            }
        }

        async sendEmailTransactionRequest(transaction: EmailCampaignTransactionRequest) {
            if (transaction) {

                let ts = new Date();

                let p4: EmailController.CreateParams = {
                    requestId: transaction.requestId,
                    accountId: ''+transaction.accountId,
                    userId: ''+transaction.userId,
                    transactionId: transaction.transactionId,
                    to: transaction.to,
                    subject: transaction.subject,
                    fromEmail: transaction.fromEmail,
                    fromName: transaction.fromName,
                    replyTo: transaction.replyTo,
                    transactionChannelType: transaction.transactionChannelType,
                    groupName: transaction.categoryId,
                    html: 'true',
                    dateCreated: transaction.dateCreated,
                    timestampCreated: transaction.dateCreated,
                    price: transaction.chargePerUnit,
                    unit: transaction.chargeableUnits,
                    transactionCost: transaction.chargeTotal,
                    transactionVersion: 1,
                    sourceIp: null,
                    chargeTotal: transaction.chargeTotal,
                    chargePerUnit: transaction.chargePerUnit,
                    chargeableUnits: transaction.chargeableUnits,
                    currencyCode: transaction.currencyCode,
                    personalization: transaction.personalization,
                    dlrEventHookType: DlrEventHookType.PT3_EMAIL_CAMPAIGN,
                    sendingEventHookType: SendingEventHookType.PT3_EMAIL_CAMPAIGN
                };

                try{
                    let result = await this.broker.call('v1.email.create', p4);
                    this.logger.info({id: getFormattedCampaignIdForLogs(transaction.categoryId), message: 'EmailCampaignTransaction Sent', value: {transactionId: transaction.transactionId, to: transaction.to}});
                    let duration = new Date().getTime() - ts.getTime();

                    statCollector.add('emailcampaign.sent', {
                        duration: duration,
                        value: 1,
                    }, {});

                    if(!result) {
                        statCollector.addOrionHealth(ServiceCode.PT3EMAIL_CAMPAIGN_QUEUEING, ServiceCode.PT3EMAIL_SERVICE, OrionHealth.error, duration);

                        throw new InvalidSendEmailRequestError('Service unavailable: EMAIL Service');
                    } else {
                        // statCollector
                        if (duration >= 100)
                            statCollector.addOrionHealth(ServiceCode.PT3EMAIL_CAMPAIGN_QUEUEING, ServiceCode.PT3EMAIL_SERVICE, OrionHealth.warning, duration);
                        else
                            statCollector.addOrionHealth(ServiceCode.PT3EMAIL_CAMPAIGN_QUEUEING, ServiceCode.PT3EMAIL_SERVICE, OrionHealth.success, duration);
                    }
                    return result;
                }
                catch (e) {
                    let duration = new Date().getTime() - ts.getTime();

                    statCollector.addOrionHealth(ServiceCode.PT3EMAIL_CAMPAIGN_QUEUEING, ServiceCode.PT3EMAIL_SERVICE, OrionHealth.error, duration);
                    this.logger.error({id: transaction.transactionId, message: 'Error Sending Request' , reason: e.message, value: {stack: e.stack, transactionId: transaction.transactionId}});
                    throw e;
                }
            }
            else {
                return null;
            }
        }

        async sendViberTransactionRequest(transaction: ViberCampaignTransactionRequest, priorityLevel: number) {
            if (transaction) {

                let ts = new Date();
                let fallbackMessage = Boolean(transaction.isFallback) === true;

                try {
                    let result;

                    if(fallbackMessage){
                        let smsParams:SmsPojo = {
                            accountId: transaction.accountId,
                            userId: transaction.userId,
                            from: transaction.fallbackFrom,
                            to: transaction.to,
                            body: transaction.fallbackMessage,
                            routingStrategy: process.env.SMS_DEFAULT_ROUTING_STRATEGY,
                            requestId: transaction.requestId,
                            transactionId: transaction.transactionId,
                            categoryId: transaction.categoryId,
                            transactionChannelType: TransactionChannelType.SMSFALLBACK,
                            // todo Replace with VIBER SMS FALLBACK EVENT HOOK TYPES
                            sendingEventHookType: SendingEventHookType.PT3_SMS_CAMPAIGN,
                            dlrEventHookType: DlrEventHookType.PT3_SMS_CAMPAIGN,
                            isTestTransaction:transaction.isTestCampaign
                        };

                        let optoutParams: GenerateOptOutLinkParams = {
                            account_id: transaction.accountId,
                            campaign_id: transaction.categoryId,
                            id: transaction.transactionId,
                            mobile_number: transaction.to,
                            sender_id: transaction.fallbackFrom
                        };

                        smsParams.body = await this.linkGenerator.generateOptOutLink(transaction.fallbackMessage, optoutParams);

                        let viewOnlineParams: GenerateViewOnlineLinkParams = {
                            account_id: transaction.accountId,
                            campaign_id: transaction.categoryId,
                            id: transaction.transactionId,
                            mobile_number: transaction.to,
                            personalData: transaction.personalData
                        };

                        smsParams.body = await this.linkGenerator.generateViewOnlineLink(smsParams.body, viewOnlineParams);

                        result = await this.broker.call("v2.sms.create", smsParams);

                    } else {

                        let p4: ViberPojo = {
                            fallbackMessage: transaction.fallbackMessage,
                            accountId: transaction.accountId,
                            userId: transaction.userId,
                            from: transaction.from,
                            fallbackFrom: transaction.fallbackFrom,
                            to: transaction.to,
                            body: transaction.text,
                            viber_ttl: transaction.viber_ttl,
                            url: transaction.url,
                            label: transaction.label,
                            image: transaction.image,
                            transactionId: transaction.transactionId,
                            requestId: transaction.requestId,
                            transactionChannelType: TransactionChannelType.VIBER,
                            categoryId: transaction.categoryId,
                            dlrEventHookType: DlrEventHookType.PT3_VIBER_CAMPAIGN,
                            sendingEventHookType: SendingEventHookType.PT3_VIBER_CAMPAIGN,
                            priorityLevel: priorityLevel,
                            routingStrategy: 'ViberRoutingStrategy',
                            isFallback: transaction.isFallback,
                            isTestCampaign: transaction.isTestCampaign
                        };

                        result = await this.broker.call("v1.viber.create", p4);

                    }

                    let duration = new Date().getTime() - ts.getTime();

                    if (!result) {
                        if (fallbackMessage)
                            statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.error, duration);
                        else
                            statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.VIBER, OrionHealth.error, duration);
                        throw new InvalidSendViberRequestError("Service unavailable: Viber Service");
                    } else {
                        this.logger.info({
                            id: getFormattedCampaignIdForSendingLogs(transaction.categoryId),
                            message: "Sent to VIBER Service",
                            value: {transactionId: transaction.transactionId, to: transaction.to}
                        });

                        /**
                         *
                         *  report to influxdb
                         *
                         * */
                        statCollector.add('vibercampaign.sent', {
                            count: 1,
                            duration: duration
                        }, {
                            accountId: transaction.accountId,
                            campaignId: transaction.categoryId
                        });

                        // Duration of call to sms service is above 100ms
                        if (duration >= 100) {
                            if (fallbackMessage)
                                statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.warning, duration);
                            else
                                statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.VIBER, OrionHealth.warning, duration);
                        }
                        else {
                            if (fallbackMessage)
                                statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.success, duration);
                            else
                                statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.VIBER, OrionHealth.success, duration);
                        }
                    }

                    return result;
                } catch (e) {
                    let duration = new Date().getTime() - ts.getTime();
                    if (fallbackMessage)
                        statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.SMS, OrionHealth.error, duration);
                    else
                        statCollector.addOrionHealth(ServiceCode.VIBER_CAMPAIGN_QUEUEING, ServiceCode.VIBER, OrionHealth.error, duration);

                    this.logger.error({
                        id: getFormattedCampaignIdForSendingLogs(transaction.categoryId),
                        message: "VIBER CAMPAIGN TRANSACTION SENDING FAILED",
                        reason: e.message,
                        value: {transactionId: transaction.transactionId, stack: e.stack}
                    });

                    throw e;
                }
            }
            else {
                return null;
            }
        }

        async updateCampaignSlack(campaign: Campaign) {
            try {
                let campaignSlackUpdate = await this.campaignSlackUpdateDao.find(campaign.id);
                if (!campaignSlackUpdate) {
                    await clearCachedCall(this.campaignSlackUpdateDao.constructor.name, 'find', [campaign.id]);
                }
                let aggregatedCampaignTransactionSource = await this.campaignDao.aggregateCampaignTransactionSource(campaign.id, campaign.accountId);
                let result = await this.slack.updateCampaignSlack(campaignSlackUpdate, campaign, aggregatedCampaignTransactionSource);
                await this.campaignSlackUpdateDao.findOrCreate(result);
            } catch (e) {
            }
        }

        async addToCampaignSlackThread(campaign: Campaign, text: string, color: string) {
            try {
                let campaignSlackUpdate = await this.campaignSlackUpdateDao.find(campaign.id);
                if (!campaignSlackUpdate) {
                    await clearCachedCall(this.campaignSlackUpdateDao.constructor.name, 'find', [campaign.id]);
                }
                if (campaignSlackUpdate) {
                    await this.slack.addToCampaignSlackThread(campaignSlackUpdate, text, color);
                }
            } catch (e) {
            }
        }

    }
}

export function prepareFilebasedTransactions(campaign: Campaign, data: any[]) {
    if (campaign.campaignTypeCode === CampaignTypeCodeEnum.COMPOSE_BLAST) {
        data = data.map(renameFields).map((e) => {
            return {
                to: e.mobile_number,
                from: campaign.senderIdName,
                message: interpolateMessage(e, campaign.message)
            };
        });
    }
    return data.map((e, i) => {
        e.identifier = i;
        return e;
    });
}
