'use strict';

angular.module('mdbxIoApp')
  .service('ViewOnlineViberService', ['$http',
    function($http) {
      // AngularJS will instantiate a singleton by calling "new" on this function
      var service = this;

      this.getViberOrion = function(params) {
        service.busy = true;
        return $http({
          url: '/api/view-online-viber/view-online-viber-orion-get',
          method: 'GET',
          params: params
        }).then(function(response) {
          service.busy = false;
          return response;
        });
      };
    }
  ]);
