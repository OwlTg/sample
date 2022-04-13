'use strict';

angular.module('mdbxIoApp')
  .config(function($stateProvider) {
    $stateProvider
      .state('view-online-viber-orion', {
        url: '/view-online-viber-orion?key&id&campaignId&mobile&hash&type',
        templateUrl: 'app/view-online-viber-orion/view-online-viber-orion.html',
        controller: 'ViewOnlineViberOrionCtrl',
        controllerAs: 'viewOnlineViberOrion'
      })
      .state('view-online-viber-link-expired', {
        url: '/view-online-viber-link-expired',
        templateUrl: 'app/view-online-viber-orion/view-online-viber-link-expired.html',
        controller: 'ViewOnlineViberLinkExpired'
      })

  });
