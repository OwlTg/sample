(function () {
    'use strict';

    angular.module('chatApp')
        .controller('TicketsController', ['$scope','$state', 'ticketsService', 'socketService', '$stateParams', '$window', '$timeout', '$uibModal', '$uibModalStack',
            function ($scope, $state, ticketsService, socketService, $stateParams, $window, $timeout, $uibModal, $uibModalStack) {
                var ctrl = this;
                ctrl.newReassignment = false;
                $scope.showNotification = false;
                this.queuedTickets = [];
                this.state = {
                    activeTickets: {
                        readyForNewTickets: false
                    },
                    queuedTickets: {
                        readyForNewTickets: false
                    }
                };

                ctrl.service = ticketsService;
                ctrl.disableReassignButton = false;
                ctrl.activeReassignTicketModal = null;
                ctrl.isOpenReassignModal = false;
                ctrl.loadedTicket = null;
                ctrl.currentBlankState = null;
                ctrl.hasChatHistory = false;

                ctrl.loadBlankState = function(reload) {

                    setTimeout(function() {
                        ctrl.service.getAllMessageCount()
                            .then(function (response) {
                                var type = 'New User';
                                ctrl.hasChatHistory = response.data;
                                var isSameBlankState = false;
                                if (ctrl.hasChatHistory == true) {
                                    type = 'Existing User';
                                }
                                ctrl.user_type = type;
                                var prevBlankState = ctrl.currentBlankState;
                                ctrl.setCurrentBlankState(ctrl.activeTickets.length, ctrl.queuedTickets.length);
                                var newBlankState = ctrl.currentBlankState;
                                if(ctrl.currentBlankState != null && prevBlankState == newBlankState){
                                    isSameBlankState = true ;
                                }
                                if (reload && !isSameBlankState) {
                                    $timeout(function () {
                                        ctrl.user_type = type;
                                        ctrl.getUserDetails();
                                        if (ctrl.activeTickets.length == 0) {
                                            $state.go('tickets.ticket', {ticketId: 0});
                                        } else {
                                            ctrl.goToActiveTicket();
                                        }
                                        ctrl.activeTickets = ctrl.activeTickets;
                                        ctrl.queuedTickets = ctrl.queuedTickets;
                                    });
                                }
                                ctrl.getUserDetails();
                                if (ctrl.activeTickets.length == 0) {
                                    $state.go('tickets');
                                }

                            });
                    } );
                }

                ctrl.getUserDetails = function() {
                    ctrl.service.getUserDetails().then(function (response) {
                        ctrl.user_name = response.data.userName;
                        ctrl.user_email = response.data.userName;
                        if(response.data.userName == "" || response.data.userName == " "){
                            ctrl.user_name = response.data.userEmail;
                            ctrl.user_email = response.data.userEmail;
                        }

                        ctrl.agentId = response.data.userId;
                    });
                }



                ctrl.setCurrentBlankState = function(activeTickets, queuedTickets) {
                    if(ctrl.user_type == "New User" ){
                        if(activeTickets == 0 && queuedTickets == 0) {
                            ctrl.currentBlankState = "Get Started";
                        } else if (activeTickets == 0 && queuedTickets > 0){
                            ctrl.currentBlankState = "Customer Waiting";
                        } else {
                            ctrl.currentBlankState = null;
                        }

                    } else { // existing
                        if(activeTickets == 0 && queuedTickets == 0){
                            ctrl.currentBlankState = "Good Job";
                        } else if (activeTickets == 0 && queuedTickets > 0){
                            ctrl.currentBlankState = "Customer Waiting";
                        } else {
                            ctrl.currentBlankState = null;
                        }
                    }
                }


                ctrl.service.getChatEnabled().then(function(response) {
                    if(!response.data.chatEnabled) $window.location.href = '/chat/teaser';
                })

                socketService.on('chat:newMessage', function (data) {
                    ctrl.activeTickets.forEach(function(ticket) {
                        if (ticket.id === data.ticketId) {
                            ticket.lastMessage = data;
                            ticket.lastMessageTimestamp = data.dateCreated;
                        }
                    });
                });

                socketService.on('chat:newTicket', function (data) {
                    ctrl.updateQueuedTickets(data);
                    if (ctrl.activeTickets.length == 0 || ctrl.queuedTickets.length == 0) {
                        ctrl.loadBlankState(true);
                    }
                });


                socketService.on('chat:messageCount', function (data) {
                    if (ctrl.agentId === data.agentId) {
                        var activeListIndex = ctrl.activeTickets.findIndex(function (t) {
                            return t.id === data.ticketId
                        });
                        if (ctrl.activeTickets.length > 0) {
                            ctrl.activeTickets[activeListIndex].unreadMessageCount = data.messageCount;
                        }
                    }

                });

                socketService.on('chat:assignedTicket', function(data) {
                    ctrl.removeFromQueued(data);
                    if (ctrl.activeTickets.length == 0 || ctrl.queuedTickets.length == 0) {
                        ctrl.loadBlankState(true);
                    }
                });


                socketService.on('chat:reassignmentNotification', function(data) {

                    if (ctrl.agentId === data.agentId) {
                        if (ctrl.agentId === data.newAgentId) {
                            if (data.notificationType === 'PendingReassignment') {
                                ctrl.addToNotification(data);

                            } else if (data.notificationType === 'AcceptReassignment') {
                                ctrl.removeFromNotification(data.id);
                                ctrl.addToNotification(data);

                                ctrl.updateActiveTickets(data);

                            } else if (data.notificationType === 'DeclinedReassignment') {
                                ctrl.removeFromNotification(data.id);
                                ctrl.addToNotification(data);

                            } else if (data.notificationType === 'ExpiredReassignment') {
                                ctrl.removeFromNotification(data.id);

                                if (ctrl.activeReassignTicketModal && ctrl.activeReassignTicketModal === data.id) {
                                    $uibModalStack.dismissAll();
                                    ctrl.openReassignErrorModal('expired');
                                }
                            }
                        }

                        if (ctrl.agentId === data.previousAgentId) {
                            if (data.notificationType === 'AcceptReassignment') {
                                ctrl.addToNotification(data);

                                var index = ctrl.removeFromActive(data.id);
                                if (index !== undefined && $state.current.name === 'tickets.ticket' && ctrl.loadedTicket.id === data.id) {
                                    ctrl.goToActiveTicket(index);
                                }

                            } else if (data.notificationType === 'DeclinedReassignment' || data.notificationType === 'ExpiredReassignment') {
                                ctrl.removeFromNotification(data.id);
                                ctrl.addToNotification(data);
                                ctrl.togglePendingReassignment(data, false);

                            }
                        }
                    }
                    var isSameBlankState = false;
                    var prevBlankState = ctrl.currentBlankState;
                    ctrl.setCurrentBlankState(ctrl.activeTickets.length, ctrl.queuedTickets.length);
                    var newBlankState = ctrl.currentBlankState;
                    if(ctrl.currentBlankState != null && prevBlankState == newBlankState){
                        isSameBlankState = true ;
                    }
                    if ((ctrl.activeTickets.length == 0 || ctrl.queuedTickets.length == 0) && !isSameBlankState) {
                        ctrl.loadBlankState(true);
                    }
                });

                socketService.on('chat:requeueNotification', function(data) {
                    ctrl.removeFromNotification(data.id);
                    ctrl.isRequeued = true;
                    if (ctrl.activeReassignTicketModal && ctrl.activeReassignTicketModal === data.id || ctrl.isOpenReassignModal) {
                        $uibModalStack.dismissAll();
                        ctrl.openReassignErrorModal('requeued');
                        ctrl.isOpenReassignModal = false;
                    }
                    if (ctrl.agentId === data.agentId) {
                        ctrl.addToNotification(data);
                    }
                        var index = ctrl.removeFromActive(data.id);
                            ctrl.goToActiveTicket(index);
                });

                socketService.on('chat:closedTicket', function(data) {
                    ctrl.removeFromNotification(data.id);

                    if(ctrl.activeReassignTicketModal && ctrl.activeReassignTicketModal === data.id){
                        $uibModalStack.dismissAll();
                        ctrl.openReassignErrorModal('closed');
                    }
                });

                ctrl.togglePendingReassignment = function(ticketData, state) {
                    var activeListIndex = ctrl.activeTickets.findIndex(function (t) {
                        return t.id === ticketData.id;
                    });
                    ctrl.activeTickets[activeListIndex].hasPendingReassignment = state;

                    if(ctrl.loadedTicket.id === ticketData.id){
                        ctrl.disableReassignButton = state;
                    }
                };

                ctrl.service.getNotificationList().then(function(response) {
                    ctrl.notificationList = response.data;
                });

                ctrl.addToNotification = function(notif) {
                    ctrl.notificationList.unshift(notif);
                    $scope.showNotification = true;
                };

                ctrl.removeFromNotification = function(ticketId) {
                    $('.tooltip').tooltip('hide');
                    ctrl.notificationList = ctrl.notificationList.filter(function(t) {
                        return t.id !== ticketId || (t.id === ticketId && t.notificationType !== 'PendingReassignment');
                    });
                };

                ctrl.activeTickets = [];

                ctrl.queuedTickets = [];

                ctrl.messageCount = {};

                ctrl.firstLoad = true;

                ctrl.isRequeued = false;

                ctrl.currentTicketId = 0;


                ctrl.clickReassignNotif = function(chatLogId) {
                    ctrl.service.clicked({
                        chatLogId: chatLogId
                    }).then(function(response) {
                        ctrl.notificationList.filter(function(e) {
                            if(e.chatLogId === chatLogId) {
                                e.clicked = true;
                                ctrl.reloadAllTickets(e);
                                return e;
                            }
                        })

                        ctrl.service.getUnclickedCount().then(function(response) {
                            ctrl.unclickedCount = response.data;
                        });
                    })
                }

                ctrl.getActiveTicket = function(ticketId) {
                    if (ctrl.activeTickets && ctrl.activeTickets.length) {
                        var activeTicket = ctrl.activeTickets.findIndex(function (t) {
                            return t.id === parseInt(ticketId);
                        });
                        if (activeTicket > -1) {
                            return ctrl.activeTickets[activeTicket];
                        }
                    }
                };

                ctrl.goToActiveTicket = function(index) {
                    if (!index) {
                        index = 0;
                    }
                    if (ctrl.loadedTicket===null) {
                        if(ctrl.activeTickets.length>0){
                            setTimeout(function () {
                                $state.go('tickets.ticket', {ticketId: ctrl.activeTickets[0].id});
                            }, 100);
                        }else {
                            setTimeout(function () {

                                if(ctrl.activeTickets.length > 0){
                                    $state.go('tickets.ticket', {ticketId: ctrl.activeTickets[0].id});
                                } else {
                                    $state.go('tickets');
                                }

                            }, 200);
                        }
                    } else {
                    var activeIndex = ctrl.activeTickets.findIndex(function (ticket) {
                        return ticket.id === ctrl.loadedTicket.id;
                    });
                    var ticketId;
                    if (ctrl.activeTickets && ctrl.activeTickets.length) {
                        if (ctrl.activeTickets.length - 1 >= index) {
                            ticketId = ctrl.activeTickets[index].id;
                        }
                        if (ticketId) {
                            if (ticketId !== activeIndex && activeIndex !== -1) {
                                setTimeout(function () {
                                    $state.go('tickets.ticket', {ticketId: ctrl.activeTickets[activeIndex].id});
                                });

                            } else {
                                setTimeout(function () {
                                    if(ctrl.user_type = 'New User'){
                                        let index = (activeIndex <= 0)? 0 : activeIndex;
                                        $state.go('tickets.ticket', {ticketId: ctrl.activeTickets[index].id});
                                    } else {
                                        $state.go('tickets.ticket', {ticketId: ctrl.activeTickets[index].id});
                                    }
                                }, 100);
                            }

                        } else {
                            if (activeIndex !== -1 && ctrl.activeTickets[activeIndex].id === ctrl.loadedTicket.id) {
                                setTimeout(function () {
                                    $state.go('tickets.ticket', {ticketId: ctrl.activeTickets[activeIndex].id});
                                },100)
                            } else {
                                setTimeout(function () {
                                    $state.go('tickets.ticket', {ticketId: ctrl.activeTickets[ctrl.activeTickets.length - 1].id});
                                }, 100);
                            }
                        }
                    } else {
                        $state.go('tickets');
                    }
                }
                };

                ctrl.removeFromActive = function(ticketId) {
                    var activeIndex = ctrl.activeTickets.findIndex(function (ticket) {
                        return ticket.id === ticketId;
                    });
                    if (activeIndex > -1) {
                        ctrl.activeTickets.splice(activeIndex, 1);
                        ctrl.messageCount.active--;
                        return activeIndex;
                    }
                };

                ctrl.removeFromQueued = function(data) {
                    let queuedTicketIndex = ctrl.queuedTickets.findIndex(function(ticket) {
                        return data.id === ticket.id;
                    });

                    if (queuedTicketIndex>-1) {
                        ctrl.queuedTickets.splice(queuedTicketIndex, 1);
                        ctrl.messageCount.queued--;
                    }
                };

                ctrl.getNextQueuedChat = function() {
                    ticketsService.getNextQueuedChat().then(function(response) {
                        var ticket = response.data;
                        if (ticket) {
                            $state.go('tickets.ticket', {
                                ticketId: ticket.id
                            });

                            ctrl.removeFromQueued(ticket);
                            ctrl.updateActiveTickets(ticket);
                        }
                    });
                };

                //get active tickets on load
                ctrl.getActiveTickets = function() {
                    var activeLastTicketId;

                    //get ticket id
                    if(ctrl.activeTickets.length > 0) {
                        activeLastTicketId = ctrl.activeTickets[ctrl.activeTickets.length - 1].id;
                    }

                    ticketsService.getTicketList({
                        status: "active",
                        afterTicketId: activeLastTicketId,
                        limit: 10
                    }).then(function(response) {
                        var tickets = response.data && response.data.ticketList;
                        if (tickets) {
                            tickets.forEach(function (value) {
                                value.hasPendingReassignment = value.notificationDetails && value.notificationDetails.notificationType === 'PendingReassignment';
                                ctrl.activeTickets.push(value);
                            });
                            if (ctrl.firstLoad === true) {
                                ctrl.firstLoad = false;
                                if (!document.referrer.toString().includes("/chat")) {
                                    ctrl.goToActiveTicket(0)
                                }
                            }
                            /*if (!$scope.ticketsCtrl) {
                                ctrl.goToActiveTicket(0);
                            }*/
                        }
                        ctrl.state.activeTickets.readyForNewTickets = !response.data.hasNext;

                    }).then(function(){
                        ctrl.loadBlankState(false);
                    });
                };


                //get queued tickets on load
                ctrl.getQueuedTickets = function() {
                    var queuedLastTicketId;

                    //get ticket id
                    if(ctrl.queuedTickets.length > 0) {
                        queuedLastTicketId = ctrl.queuedTickets[ctrl.queuedTickets.length - 1].id;
                    }

                    ticketsService.getTicketList({
                        status: "queued",
                        afterTicketId: queuedLastTicketId,
                        limit: 10
                    }).then(function(response) {
                        var tickets = response.data && response.data.ticketList;
                        if (tickets) {
                            tickets.forEach(function (value) {
                                ctrl.queuedTickets.push(value);
                            });
                        }
                        ctrl.state.queuedTickets.readyForNewTickets = !response.data.hasNext;
                    });
                };

                ctrl.updateQueuedTickets = function (data) {
                    ctrl.messageCount.queued++;
                    if(ctrl.queuedTickets.length>0) {
                        if (ctrl.state.queuedTickets.readyForNewTickets && ctrl.queuedTickets[ctrl.queuedTickets.length - 1].id < data.id) {
                            ctrl.queuedTickets.push(data);
                        } else {
                            ticketsService.getTicketList({
                                status: 'queued'
                            }).then(function (response) {
                                var tickets = response.data && response.data.ticketList;
                                if (tickets) {
                                    ctrl.queuedTickets = tickets;
                                }

                                ctrl.state.queuedTickets.readyForNewTickets = !response.data.hasNext;
                            });
                        }
                    }else {
                        ctrl.queuedTickets.push(data);}
                };

                if (!$stateParams.ticketId) {
                    ctrl.goToActiveTicket(0);
                }

                //get queued tickets on load
                ctrl.getMessageCount = function() {

                    ticketsService.getMessageCount()
                        .then(function(response) {
                        var ticket = response.data;
                        ctrl.messageCount = ticket
                    });
                };

                ctrl.updateActiveTickets = function(data) {
                    if(ctrl.agentId === data.agentId){
                        ctrl.messageCount.active++;

                        var activeTicketCount = ctrl.activeTickets.length;
                        var activeLastTicketId = activeTicketCount > 0 ? ctrl.activeTickets[ctrl.activeTickets.length - 1].id : null;

                        if (!activeTicketCount || (ctrl.state.activeTickets.readyForNewTickets && activeLastTicketId && activeLastTicketId < data.id)) {
                            ctrl.activeTickets.push(data);
                        } else {
                            ticketsService.getTicketList({
                                status: 'active',
                                beforeTicketId: activeLastTicketId + 1
                            }).then(function (response) {
                                var tickets = response.data && response.data.ticketList;
                                if (tickets) {
                                    ctrl.activeTickets = tickets;
                                }

                                ctrl.state.activeTickets.readyForNewTickets = !response.data.hasNext;
                            });
                        }
                    }
                };

                ctrl.reloadAllTickets = function(data) {
                    if(ctrl.agentId === data.agentId){
                        if(data.id > ctrl.activeTickets[ctrl.activeTickets.length-1].id) {
                            ticketsService.getTicketList({
                                status: 'active',
                                beforeTicketId: data.id + 1
                            }).then(function(response) {
                                var tickets = response.data && response.data.ticketList;

                                if (tickets) {
                                    ctrl.activeTickets = tickets;
                                }

                                ctrl.state.activeTickets.readyForNewTickets = !response.data.hasNext;
                            });
                        }
                    }
                };

                ctrl.reassignmentFeedback = function(data, feedback) {
                    ctrl.service.acceptDeclineReassignment({
                        chatId: data.chatId,
                        ticketId: data.id,
                        oldAgentId: data.previousAgentId,
                        notificationId: data.notificationId,
                        reassignResponse: feedback
                    }).then(function(response) {
                        if(response && response.data.ticket.notificationType === 'AcceptReassignment') {
                            $state.go('tickets.ticket', { ticketId: response.data.ticket.id });

                            setTimeout(function() {
                                var el = document.getElementsByClassName('active_chat');
                                el[0].scrollIntoView({ behavior: 'smooth', block: 'end', inline: 'center' });
                            }, 1000);
                        }

                    }).catch(function(response) {
                        if(response && response.data) {
                            var errorType = (response.data.message === 'Ticket Already Closed') ? 'closed' : 'expired';
                            ctrl.openReassignErrorModal(errorType);
                        }
                    });
                };

                ctrl.toggleNotification = function() {
                    if($scope.showNotification === true){
                        var deleteNotifIds = [];
                        ctrl.notificationList = ctrl.notificationList.filter(function (ticket) {
                            if(ticket.notificationType !== 'PendingReassignment') {
                                deleteNotifIds.push(ticket.notificationId);
                            }
                            return ticket.notificationType === 'PendingReassignment';
                        });

                        if(deleteNotifIds.length){
                            ctrl.service.removeNotification({notificationId: deleteNotifIds});
                        }
                    }

                    $scope.showNotification = !$scope.showNotification;
                };

                ctrl.openReassignErrorModal = function(error){
                    $uibModal.open({
                        templateUrl: 'chat-app/components/modal/reassign-error.html',
                        backdrop: 'static',
                        controller: ['$scope', '$uibModalInstance', function($scope, $uibModalInstance){
                            $scope.errorState = error;
                            $scope.close = function() {
                                $uibModalInstance.dismiss('close');
                            };
                        }]
                    });
                };

                ctrl.openResetPasswordModal = function(error){
                    $uibModal.open({
                        templateUrl: 'chat-app/components/modal/reset-password.html',
                        size: 'lg',
                        scope: $scope,
                        backdrop: 'static',
                        controller: 'ResetPasswordController',
                        controllerAs: 'resetPasswordCtrl',

                    });
                };

                //ctrl.loadBlankState(false);
                ctrl.getActiveTickets();
                ctrl.getQueuedTickets();
                ctrl.getMessageCount();
            }
        ]);
})();
