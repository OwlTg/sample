(function() {
    'use strict';

    angular.module('chatApp')
        .filter('reverse', function() {
            return function(items) {
                return items.slice().reverse();
            }
        })
        .directive('chatBox', ['$rootScope', '$timeout', 'socketService', 'ticketsService', '$document',
            function($rootScope, $timeout, socketService, ticketsService,  $document) {
                return {
                    templateUrl: 'chat-app/components/chat-box/chat-box.html',
                    scope: {
                        'messages': '=',
                        'ctrl': '=',
                        'messagesState': '=',
                        'getNextQueuedFn': '='
                    },
                    restrict: 'EA',
                    link: function(scope, element, attrs) {
                        scope.getClass = function(message) {
                            if (message.messageTypeCode === 'outbound') {
                                return 'outgoing_msg';
                            } else if (message.messageTypeCode === 'inbound') {
                                return 'incoming_msg';
                            }
                        };

                        scope.textMessage = '';
                        scope.charLimit = 3000;
                        scope.scrollPercent = 100;
                        scope.blankStateType = 'Existing User';
                        scope.getPreviousMessages = function() {
                            scope.ctrl.getMessagesBefore(scope.messages[0] && scope.messages[0].id);
                        };
                        scope.getNextMessages = function() {
                            let limit = 10;
                            scope.ctrl.getMessagesAfter(scope.messages[scope.messages.length - 1] && scope.messages[scope.messages.length - 1].id, limit);
                        };

                        scope.breakByNewLine = function(text) {
                            if(text)
                            return text.split('\n');
                        };

                        var el = document.getElementById('msg_history');
                        el.addEventListener('scroll', function(e) {
                            if(this.scrollTop === 0) {
                                scope.getPreviousMessages();
                            } else if (Math.round(this.scrollTop + $(el).height()) === Math.round(this.scrollHeight)) {
                                scope.getNextMessages();
                            }
                            let scrollGapPercent = (this.scrollTop / (this.scrollHeight - this.offsetHeight)) * 100;
                            scope.scrollPercent = scrollGapPercent;
                        });

                        scope.updateChatBox= function(action) {
                            let activeTickets = scope.$parent.ticketsCtrl.activeTickets.length;
                                $timeout(function() {
                                    scope.queuedTickets = scope.$parent.ticketsCtrl.queuedTickets.length
                                    scope.activeTickets = scope.$parent.ticketsCtrl.activeTickets.length
                                    scope.user_name = scope.$parent.ticketsCtrl.user_name;
                                    scope.user_type = scope.$parent.ticketsCtrl.user_type;
                                    if(scope.user_name == "" || scope.user_name == " "){
                                        scope.user_name = scope.$parent.ticketsCtrl.user_email;
                                    }
                                    if(scope.user_type == "New User" && (scope.queuedTickets <= 0 && scope.activeTickets <= 0)){//new user
                                        scope.blankStateType = "New User";
                                    } else if(scope.user_type == "Existing User" && (scope.queuedTickets <= 0 && scope.activeTickets <= 0)){//existing user
                                        scope.blankStateType = "Existing User";
                                    } else if(scope.activeTickets <= 0 && scope.queuedTickets > 0){
                                        scope.blankStateType = "Customer Waiting";
                                    } else {
                                        scope.blankStateType = "";
                                    }
                                }, 300);
                        };

                        socketService.on('chat:newTicket', function (data) {
                            scope.activeTickets = scope.$parent.ticketsCtrl.activeTickets.length
                            scope.queuedTickets = scope.$parent.ticketsCtrl.queuedTickets.length
                            if(scope.activeTickets == 0 || scope.queuedTickets){
                                scope.updateChatBox('New Ticket');
                            }
                        });

                        socketService.on('chat:assignedTicket', function(data) {
                            scope.activeTickets = scope.$parent.ticketsCtrl.activeTickets.length
                            scope.queuedTickets = scope.$parent.ticketsCtrl.queuedTickets.length
                            if(scope.activeTickets == 0 || scope.queuedTickets){
                                scope.updateChatBox('New Ticket');
                            }
                        });
                        socketService.on('chat:reassignmentNotification', function(data) {
                            scope.activeTickets = scope.$parent.ticketsCtrl.activeTickets.length
                            scope.queuedTickets = scope.$parent.ticketsCtrl.queuedTickets.length
                            if(scope.activeTickets == 0 || scope.queuedTickets){
                                scope.updateChatBox('New Ticket');
                            }
                        });

                        scope.getNextQueuedChat = function () {
                            scope.$parent.ticketsCtrl.getNextQueuedChat();
                        }

                        scope.showInboundUserPhoto = function(index) {
                            if (scope.messages[index].messageTypeCode === 'inbound') {
                                if (index > 0) {
                                    if (scope.messages[index].messageTypeCode !== scope.messages[index - 1].messageTypeCode) {
                                        return true;
                                    } else if (scope.showDateGroup(index)) {
                                        return true;
                                    }
                                } else {
                                    return true;
                                }
                            } else {
                                return false;
                            }
                        };

                        // scope.keyDownEvent = function($event) {
                        //     if($event.keyCode === 13) {
                        //         $event.preventDefault();
                        //         scope.sendOutbound();
                        //     }
                        // }

                        scope.showDateGroup = function(index) {
                            if (index > 0) {
                                if (moment(scope.messages[index].dateCreated).startOf('day').diff(moment(scope.messages[index-1].dateCreated).startOf('day').toDate(), 'days') !== 0) {
                                    return true;
                                }
                            } else {
                                return true;
                            }
                        };
                        scope.getDateGroup = function(message) {
                            if (moment().diff(moment(message.dateCreated).startOf('day'), 'days') === 0) {
                                return 'Today';
                            }
                            else if(moment().diff(moment(message.dateCreated).startOf('day'), 'days') === 1) {
                                return 'Yesterday';
                            }
                            else {
                                return moment(message.dateCreated).format('MMMM DD, YYYY');
                            }
                        };

                        scope.isSendDisabled = function() {
                            return !scope.textMessage;
                        };

                        scope.isMediaDisabled = function() {
                            return scope.textMessage;
                        };

                        scope.getDlrStatusName = function(status) {
                            if(status == 'expired'){
                                return 'Bounced'
                            }else if(status === 'delivered'){
                                return 'Delivered'
                            }else if(status === 'pending'){
                                return 'Sending'
                            }else if(status === 'rejected'){
                                return 'Bounced'
                            }else if(status === 'rejectedcountry'){
                                return 'RejectedCountry'
                            }
                        }

                        scope.sendOutbound = function() {
                            var result = scope.ctrl.sendOutbound(scope.textMessage, 'viber');
                            if(result === true){
                                scope.textMessage = '';
                                scope.ctrl.mediaFiles = [];
                            }
                        };

                        scope.resendOutbound = function(messageId, messageText, messageTypeCode, idx) {
                            let index = scope.messages.length - idx - 1;
                            scope.ctrl.deleteMessage(messageId, index);
                            scope.ctrl.sendOutbound(messageText, messageTypeCode);
                        };

                        scope.deleteMessage = function(message, idx) {
                            let index = scope.messages.length - idx - 1;
                            var messageId = message.id;
                            scope.ctrl.deleteMessage(messageId, index);
                        };

                        scope.updateChatBox('First Load');

                        scope.removeMedia = function removeGroup(index) {
                            scope.ctrl.deleteMediaMessage(scope.ctrl.mediaFiles, index);
                        }
                    }
                };
            }
        ]);
})();
