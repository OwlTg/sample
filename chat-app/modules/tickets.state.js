(function() {
    'use strict';

    angular.module('chatApp')
        .config(function($stateProvider) {
            $stateProvider
                .state('tickets', {
                    url: '/chat/tickets',
                    templateUrl: 'chat-app/modules/tickets/tickets.html',
                    controller: 'TicketsController',
                    controllerAs: 'ticketsCtrl',
                    title: 'Tickets',
                    module: 'ticket',
                    cache: false
                });
        });
})();
