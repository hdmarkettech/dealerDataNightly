'use strict';

const AWS = require('aws-sdk');
const sfn = new AWS.StepFunctions();
module.exports.restart = (event, context, callback) =>{
    console.log('event', event)
    let StateMachineArn = event.iterator.StateMachineArn;
    let params = {
        input: JSON.stringify(event),
        stateMachineArn: StateMachineArn
    };
    sfn.startExecution(params, function(err, data) {
        if (err) callback(err);
        else callback(null,event);
    });
}
