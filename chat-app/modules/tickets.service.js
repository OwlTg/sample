(function() {
    'use strict';

    angular.module('chatApp')
        .factory('ticketsService', ['config', 'apiService',
            function(config, apiService) {
                return apiService.wrap({
                    name: 'ticketsService',
                    apiEndpoint: config.orionEndpoint,
                    url: '/tickets',
                    getUrl: {
                        getNextQueuedChat: '/pt3/chat/tickets/next',
                        getMessages: '/pt3/chat/messages',
                        getTicketDetails: '/pt3/chat/ticket',
                        getTicketList: '/pt3/chat/ticketlist',
                        getMessageCount: '/pt3/chat/count',
                        getAllMessageCount: '/pt3/chat/hasChatHistory',
                        getUserDetails: '/pt3/userDetails',
                        getChatEnabled: '/pt3/chatEnabled',
                        getAgents: '/pt3/chat/agents',
                        getNotificationList: '/pt3/chat/notificationList',
                        getUnclickedCount: '/pt3/chat/unclickedCount',
                        getResetPasswordPermission: '/pt3/chat/getResetPasswordPermission',
                        getChatMediaMessages: '/pt3/chat/getChatMediaMessages'
                    },
                    postUrl: {
                        closeTicket: '/pt3/chat/tickets/close',
                        sendOutbound: '/pt3/chat/outbound',
                        deleteMessage: '/pt3/chat/deletemessage',
                        deleteMediaMessage: '/pt3/chat/deleteMediaMessage',
                        reassignTicket: '/pt3/chat/reassignTicket',
                        clicked: '/pt3/chat/reassignmentClicked',
                        acceptDeclineReassignment: '/pt3/chat/acceptDeclineReassignTicket',
                        removeNotification: '/pt3/chat/removeNotification',
                        resetUnreadCount: '/pt3/chat/resetUnreadCount'
                    }
                });
            }
        ]);
})();
