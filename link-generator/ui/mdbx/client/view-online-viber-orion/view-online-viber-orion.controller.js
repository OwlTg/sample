'use strict';

angular.module('mdbxIoApp')
  .controller(
    'ViewOnlineViberOrionCtrl', ['$scope', '$stateParams', 'ViewOnlineViberService', '$state',
    function($scope, $stateParams, ViewOnlineViberService, $state) {
      var ctrl = this;
      const EXPIRED = 'Expired';
      $scope.ViewOnlineViberService = ViewOnlineViberService;
      // var personalData = JSON.parse($stateParams.personalData)

      ViewOnlineViberService.getViberOrion($stateParams).then(function(response) {
        if(response.data.message === EXPIRED ){
          $state.go('view-online-viber-link-expired', $stateParams)
        }else {
          var personalData = response.data.data.personalData
          var message = interpolateMessage(personalData, response.data.data.message)
          var messageBreaks = message.split('\n').map(function(e){ return identifyEmojis(e); });
          var url = validateUrl(response.data.data.url)
          ctrl.data = response.data.data;
          ctrl.data.message = message;
          ctrl.data.url = url;
          ctrl.data.components = messageBreaks;
        }
      });

      function identifyEmojis(message){
        const exemptionList = [ '(!)', '($)', '(8ball)', '(2_hearts)'];
        var regex = /\([QVTa-z\$\!_0-9]+\)/g;
        var emojilist = ["(smiley)",
          "(sad)",
          "(wink)",
          "(angry)",
          "(inlove)",
          "(yummi)",
          "(laugh)",
          "(surprised)",
          "(moa)",
          "(happy)",
          "(cry)",
          "(sick)",
          "(shy)",
          "(teeth)",
          "(tongue)",
          "(money)",
          "(mad)",
          "(flirt)",
          "(crazy)",
          "(confused)",
          "(depressed)",
          "(scream)",
          "(nerd)",
          "(not_sure)",
          "(cool)",
          "(huh)",
          "(happycry)",
          "(mwah)",
          "(exhausted)",
          "(eek)",
          "(dizzy)",
          "(dead)",
          "(straight)",
          "(yo)",
          "(wtf)",
          "(ohno)",
          "(oh)",
          "(wink2)",
          "(what)",
          "(weak)",
          "(upset)",
          "(ugh)",
          "(teary)",
          "(singing)",
          "(silly)",
          "(meh)",
          "(mischievous)",
          "(ninja)",
          "(spiderman)",
          "(batman)",
          "(devil)",
          "(angel)",
          "(heart)",
          "(heart_break)",
          "(purple_heart)",
          "(unlike)",
          "(like)",
          "(V)",
          "(fu)",
          "(clap)",
          "(rockon)",
          "(angrymark)",
          "(thinking)",
          "(zzz)",
          "(!)",
          "(Q)",
          "(diamond)",
          "(trophy)",
          "(crown)",
          "(ring)",
          "($)",
          "(hammer)",
          "(wrench)",
          "(key)",
          "(lock)",
          "(video)",
          "(TV)",
          "(tape)",
          "(trumpet)",
          "(guitar)",
          "(speaker)",
          "(music)",
          "(microphone)",
          "(bell)",
          "(koala)",
          "(sheep)",
          "(ladybug)",
          "(kangaroo)",
          "(chick)",
          "(monkey)",
          "(panda)",
          "(turtle)",
          "(bunny)",
          "(dragonfly)",
          "(fly)",
          "(bee)",
          "(bat)",
          "(cat)",
          "(dog)",
          "(squirrel)",
          "(snake)",
          "(snail)",
          "(goldfish)",
          "(shark)",
          "(pig)",
          "(owl)",
          "(penguin)",
          "(paw)",
          "(poo)",
          "(cap)",
          "(fidora)",
          "(partyhat)",
          "(cactus)",
          "(clover)",
          "(sprout)",
          "(palmtree)",
          "(christmas_tree)",
          "(mapleleaf)",
          "(flower)",
          "(sunflower)",
          "(sun)",
          "(moon)",
          "(rain)",
          "(cloud)",
          "(umbrella)",
          "(snowman)",
          "(snowflake)",
          "(relax)",
          "(flipflop)",
          "(telephone)",
          "(phone)",
          "(nobattery)",
          "(battery)",
          "(time)",
          "(knife)",
          "(syringe)",
          "(termometer)",
          "(meds)",
          "(ruler)",
          "(scissor)",
          "(paperclip)",
          "(pencil)",
          "(magnify)",
          "(glasses)",
          "(book)",
          "(letter)",
          "(weight)",
          "(muscle)",
          "(boxing)",
          "(light_bulb)",
          "(lantern)",
          "(fire)",
          "(torch)",
          "(bomb)",
          "(cigarette)",
          "(kiss)",
          "(gift)",
          "(skull)",
          "(ghost)",
          "(golf)",
          "(golfball)",
          "(football)",
          "(tennis)",
          "(soccer)",
          "(basketball)",
          "(baseball)",
          "(8ball)",
          "(beachball)",
          "(balloon1)",
          "(balloon2)",
          "(cards)",
          "(dice)",
          "(console)",
          "(chicken)",
          "(burger)",
          "(pizza)",
          "(noodles)",
          "(sushi1)",
          "(sushi2)",
          "(donut)",
          "(egg)",
          "(hotdog)",
          "(ice_cream)",
          "(popsicle)",
          "(cupcake)",
          "(croissant)",
          "(chocolate)",
          "(lollipop)",
          "(popcorn)",
          "(cake)",
          "(cherry)",
          "(banana)",
          "(watermelon)",
          "(strawberry)",
          "(grapes)",
          "(pineapple)",
          "(pea)",
          "(corn)",
          "(mushroom)",
          "(beer)",
          "(wine)",
          "(martini)",
          "(coffee)",
          "(soda)",
          "(car)",
          "(taxi)",
          "(ambulance)",
          "(policecar)",
          "(bicycle)",
          "(airplane)",
          "(ufo)",
          "(rocket)",
          "(run)",
          "(2_hearts)",
          "(alien)",
          "(anchor)",
          "(apple)",
          "(arrow_heart)",
          "(baby_bottle)",
          "(bacon)",
          "(bikini)",
          "(black_heart)",
          "(blue_flower)",
          "(blue_heart)",
          "(bouquet)",
          "(bowtie)",
          "(cake_slice)",
          "(camera)",
          "(champagne)",
          "(checkmark)",
          "(cocktail)",
          "(color_palette)",
          "(confetti_ball)",
          "(cookie)",
          "(crying)",
          "(crystal_ball)",
          "(cutlery)",
          "(dinosaur)",
          "(do_not_enter)",
          "(down_graph)",
          "(droplet)",
          "(drum)",
          "(earth)",
          "(eggplant)",
          "(eyeroll)",
          "(eyes)",
          "(fan)",
          "(first_aid)",
          "(fist)",
          "(footsteps)",
          "(fox)",
          "(full_moon)",
          "(handicap)",
          "(heart_lock)",
          "(hmm)",
          "(hotsauce)",
          "(iceskate)",
          "(inlove)",
          "(lightening)",
          "(moneybag)",
          "(octopus)",
          "(orange_heart)",
          "(over18)",
          "(paintbrush)",
          "(party_popper)",
          "(peach)",
          "(pointer)",
          "(porcupine)",
          "(prayer_hands)",
          "(racing_flag)",
          "(rainbow)",
          "(robot)",
          "(santa_hat)",
          "(shooting_star)",
          "(shrug)",
          "(spiral)",
          "(star)",
          "(stop_sign)",
          "(sunglasses)",
          "(tablet)",
          "(target)",
          "(tiara)",
          "(tornado)",
          "(trafficlight)",
          "(up_graph)",
          "(waving)",
          "(unsubscribe)",
          "(smiley)",
          "(sad)",
          "(wink)",
          "(laugh)",
          "(happy)",
          "(mad)",
          "(flirt)",
          "(crazy)",
          "(wtf)",
          "(meh)",
          "(mischievous)",
          "(heart)",
          "(muscle)",
          "(thinking)",
          "(zzz)",
          "(diamond)",
          "(trophy)",
          "(crown)",
          "(ring)",
          "(hammer)",
          "(wrench)",
          "(key)",
          "(lock)",
          "(video)",
          "(TV)",
          "(tape)",
          "(trumpet)",
          "(guitar)",
          "(cactus)",
          "(clover)",
          "(sprout)",
          "(sun)",
          "(moon)",
          "(telephone)",
          "(knife)",
          "(syringe)",
          "(letter)",
          "(weight)",
          "(torch)",
          "(bomb)",
          "(cigarette)",
          "(kiss)",
          "(cards)",
          "(dice)",
          "(console)",
          "(egg)",
          "(hotdog)",
          "(popcorn)",
          "(cake)",
          "(pineapple)",
          "(pea)",
          "(corn)",
          "(car)",
          "(taxi)",
          "(ambulance)",
          "(policecar)",
          "(confused)",
          "(depressed)",
          "(nerd)",
          "(cool)",
          "(huh)",
          "(exhausted)",
          "(dizzy)",
          "(dead)",
          "(straight)",
          "(unlike)",
          "(like)",
          "(V)",
          "(fu)",
          "(speaker)",
          "(music)",
          "(microphone)",
          "(bell)",
          "(koala)",
          "(sheep)",
          "(ladybug)",
          "(kangaroo)",
          "(chick)",
          "(monkey)",
          "(panda)",
          "(turtle)",
          "(bunny)",
          "(dragonfly)",
          "(fly)",
          "(bee)",
          "(bat)",
          "(cat)",
          "(poo)",
          "(cap)",
          "(cloud)",
          "(ruler)",
          "(scissor)",
          "(paperclip)",
          "(pencil)",
          "(magnify)",
          "(glasses)",
          "(book)",
          "(lantern)",
          "(gift)",
          "(skull)",
          "(ghost)",
          "(chicken)",
          "(burger)",
          "(pizza)",
          "(noodles)",
          "(donut)",
          "(popsicle)",
          "(cupcake)",
          "(croissant)",
          "(cherry)",
          "(banana)",
          "(watermelon)",
          "(mushroom)",
          "(coffee)",
          "(soda)",
          "(beer)",
          "(wine)",
          "(bicycle)",
          "(airplane)",
          "(ufo)",
          "(weak)",
          "(upset)",
          "(ugh)",
          "(teary)",
          "(singing)",
          "(silly)",
          "(sick)",
          "(shy)",
          "(money)",
          "(ninja)",
          "(spiderman)",
          "(batman)",
          "(clap)",
          "(dog)",
          "(squirrel)",
          "(snake)",
          "(snail)",
          "(goldfish)",
          "(shark)",
          "(pig)",
          "(owl)",
          "(penguin)",
          "(paw)",
          "(christmas_tree)",
          "(flower)",
          "(sunflower)",
          "(umbrella)",
          "(snowman)",
          "(snowflake)",
          "(relax)",
          "(flipflop)",
          "(phone)",
          "(nobattery)",
          "(battery)",
          "(time)",
          "(boxing)",
          "(golf)",
          "(golfball)",
          "(football)",
          "(tennis)",
          "(soccer)",
          "(basketball)",
          "(baseball)",
          "(beachball)",
          "(chocolate)",
          "(lollipop)",
          "(strawberry)",
          "(grapes)",
          "(martini)",
          "(rocket)",
          "(run)",
          "(yellow_heart)",
          "(corn)",
          "(car)",
          "(taxi)",
          "(ambulance)",
          "(policecar)",
          "(beer)",
          "(wine)",
          "(bicycle)",
          "(airplane)",
          "(ufo)",
          "(rocket)",
          "(run)",
          "(martini)",
          "(grapes)",
          "(strawberry)",
          "(cake)",
          "(pea)",
          "(pineapple)",
          "(martini)",
          "(popcorn)",
          "(soda)",
          "(coffee)",
          "(mushroom)",
          "(watermelon)",
          "(banana)",
          "(cherry)",
          "(croissant)",
          "(lemon)"
        ];
        var equivalent = {'(!)' : '(exclamation)', '($)' : '(dollar)', '(8ball)' : '(eightball)', '(2_hearts)': '(two_hearts)'};
        var messages = message.split(regex).map(function(e){ return {type:'text', content: e}; });
        var emojis = (message.match(regex)||[]).map(function(e){ return emojilist.includes(e) ? {type:'emoji', content:
            ( exemptionList.includes(e) ? equivalent[e] : e) } : {type:'text', content: e}; });
        var union = [messages[0]];
        emojis.forEach(function(e,index){
          union.push(e);
          union.push(messages[index+1]);
        });
        return union;
      }


      const basicFields = ['email', 'mobile_number', 'name', 'first_name', 'last_name', 'birthday', 'gender'];
      function interpolateMessage(row, message) {
        message = message || '';
        _.each(row, function(data, index)  {
          if (basicFields.find(function (e)  {
            return e == index;
          })) {
            message = message.replace(new RegExp('\\[' + index + '\\]', 'g'), data || '');
          }
          else {
            message = message.replace(new RegExp('\\[' + index + '\\]', 'g'), data || '');
            message = message.replace(new RegExp('\\[cc_' + index + '\\]', 'g'), data || '');
          }
        });

        return message;
      }

      function validateUrl(url) {
        var validUrl = url || '';
        var urlValidator = new RegExp(/^(?:(?:https?|ftp):\/\/)(?:\S+(?::\S*)?@)?(?:(?!(?:10|127)(?:\.\d{1,3}){3})(?!(?:169\.254|192\.168)(?:\.\d{1,3}){2})(?!172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2})(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5])){2}(?:\.(?:[1-9]\d?|1\d\d|2[0-4]\d|25[0-4]))|(?:(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)(?:\.(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)*(?:\.(?:[a-z\u00a1-\uffff]{2,}))\.?)(?::\d{2,5})?(?:[/?#]\S*)?$/i);

        var isUrl = urlValidator.test(url);

        if(!isUrl)
          validUrl = '//'+url;
        else
          validUrl = url;

        return validUrl;
      }

      ctrl.removeParentheses = function(str) {
        return str.slice(1,-1);
      }
    }
  ]);

