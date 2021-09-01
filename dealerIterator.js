'use strict';
const Airtable = require('airtable');
const process = require('process');
const csv = require("csvtojson");
const AWS = require('aws-sdk');
const S3 = new AWS.S3()
const request = require('request');
let decrypted;
const encrypted = process.env['AIRTABLEAPIKEY'];
module.exports.dealerIterator = (event, context, callback) =>{
    if (decrypted) {
        processEvent(event, context, callback);
    } else {
        // Decrypt code should run once and variables stored outside of the
        // function handler so that these are decrypted once per container
        const kms = new AWS.KMS();
        kms.decrypt({ CiphertextBlob: new Buffer(encrypted, 'base64') }, (err, data) => {
            if (err) {
                console.log('Decrypt error:', err);
                return callback(err);
            }
            decrypted = data.Plaintext.toString('ascii');
            processEvent(event, context, callback);
        });
    }
}
function processEvent(event, context, callback) {
    console.log('event', event)
    let index = event.iterator.index;
    let runCount = event.iterator.runCount;
    let dataParams = {
        Bucket: 'nightlydealerdata',
        Key: 'Hunterdouglas_DealerLocator.txt'
    }
    console.log('Getting list from S3...')
    Airtable.configure({
        endpointUrl: 'https://api.airtable.com',
        apiKey: decrypted
    });
    const base = Airtable.base('app4qgZtb5X0BGxkW');
    S3.getObject(dataParams, (err, data)=>{
        if(err) {
            console.log('error from S3', err)
            return err
        }
        csv({delimiter: "\t"}).fromString(data.Body.toString('utf-8'))
            .then((JSONObj)=>{
                let dealer = JSONObj[index];
                if(dealer){
                    if(dealer.HD_Website){
                        console.log('Retrieving Dealer Website Status Code');
                        let url;
                        if(dealer.HD_Website.indexOf('http') > -1){
                            url = dealer.HD_Website;
                        } else{
                            url = `http://${dealer.HD_Website}/`
                        }
                        console.log('url', url);
                        request(url, function (error, response) {
                            if(error){
                                console.log('error:', error); // Print the error if one occurred
                                dealer["Status"] = error.code ? error.code : error.reason;
                            } else {
                                dealer["Status"] = response.statusCode.toString();
                            }
                            console.log(url, 'statusCode:', response && response.statusCode); // Print the response status code if a response was received
                            console.log('Retrieving ', dealer.Name, ' Airtable record');
                            base('Dealers (Updated Daily)').select({
                                view: "View All",
                                filterByFormula: `IF(Account_Number = "${dealer.Account_Number}", TRUE(), FALSE())`
                            }).eachPage((records, fetchNextPage)=>{
                                if(records[0]){
                                    console.log('Record found', dealer.Account_Number)
                                    console.log('Updating', dealer.Name,' in Airtable')
                                    dealer.Last_Updated = new Date();
                                    base('Dealers (Updated Daily)').update(records[0].getId(), dealer, (err, record)=>{
                                        if (err) {
                                            console.error(err);
                                        }
                                        console.log('Finished updating ', record.get('Name'))
                                        console.log('Done')
                                        index++;
                                        let restart = false;
                                        let countReached = false;
                                        if(index !== 0 && index%10 === 0){
                                            restart = true;
                                        }
                                        if(index > runCount){
                                            restart = false;
                                            countReached = true;
                                        }
                                        callback(null, {
                                            index,
                                            runCount,
                                            restart: restart,
                                            countReached: countReached,
                                            StateMachineArn: "arn:aws:states:us-east-1:467586748896:stateMachine:DealerCount"
                                        })
                                    })
                                } else{
                                    console.log('No record found in Airtable');
                                    console.log('Creating record...')
                                    base('Dealers (Updated Daily)').create(dealer, (err, record)=>{
                                        if (err) {
                                            console.error(err);
                                        }
                                        console.log('New Dealer: ', record.get('Name'))
                                        index++
                                        let restart = false;
                                        let countReached = false;
                                        if(index !== 0 && index%10 === 0){
                                            restart = true;
                                        }
                                        if(index > runCount){
                                            restart = false;
                                            countReached = true;
                                        }
                                        callback(null, {
                                            index,
                                            runCount,
                                            restart: restart,
                                            countReached: countReached,
                                            StateMachineArn: "arn:aws:states:us-east-1:467586748896:stateMachine:DealerCount"
                                        })
                                    })
                                }
                            }, (err) => {
                                if (err) {
                                    console.log('Airtable err', err);
                                    return err;
                                }
                            })
                        });
                    } else{
                        console.log('Retrieving ', dealer.Name, ' Airtable record');
                        base('Dealers (Updated Daily)').select({
                            view: "View All",
                            filterByFormula: `IF(Account_Number = "${dealer.Account_Number}", TRUE(), FALSE())`
                        }).eachPage((records, fetchNextPage)=>{
                            if(records[0]){
                                console.log('Record found', dealer.Account_Number)
                                console.log('Updating ', dealer.Name,' in Airtable')
                                dealer.Last_Updated = new Date();
                                base('Dealers (Updated Daily)').update(records[0].getId(), dealer, (err, record)=>{
                                    if (err) {
                                        console.error(err);
                                    }
                                    console.log('Finished updating ', record.get('Name'))
                                    console.log('Done')
                                    index++;
                                    let restart = false;
                                    let countReached = false;
                                    if(index !== 0 && index%10 === 0){
                                        restart = true;
                                    }
                                    if(index > runCount){
                                        restart = false;
                                        countReached = true;
                                    }
                                    callback(null, {
                                        index,
                                        runCount,
                                        restart: restart,
                                        countReached: countReached,
                                        StateMachineArn: "arn:aws:states:us-east-1:467586748896:stateMachine:DealerCount"
                                    })
                                })
                            } else{
                                console.log('No record found in Airtable');
                                console.log('Creating record...')
                                base('Dealers (Updated Daily)').create(dealer, (err, record)=>{
                                    if (err) {
                                        console.error(err);
                                    }
                                    console.log('New Dealer: ', record.get('Name'))
                                    index++
                                    let restart = false;
                                    let countReached = false;
                                    if(index !== 0 && index%10 === 0){
                                        restart = true;
                                    }
                                    if(index > runCount){
                                        restart = false;
                                        countReached = true;
                                    }
                                    callback(null, {
                                        index,
                                        runCount,
                                        restart: restart,
                                        countReached: countReached,
                                        StateMachineArn: "arn:aws:states:us-east-1:467586748896:stateMachine:DealerCount"
                                    })
                                })
                            }
                        }, (err) => {
                            if (err) {
                                console.log('Airtable err', err);
                                return err;
                            }
                        })
                    }
                } else{
                    console.log('No dealer found at index', index)
                    let restart = false;
                    let countReached = false;
                    if(index !== 0 && index%10 === 0){
                        restart = true;
                    }
                    if(index > runCount){
                        restart = false;
                        countReached = true;
                    }
                    callback(null, {
                        index,
                        runCount,
                        restart: restart,
                        countReached: countReached,
                        StateMachineArn: "arn:aws:states:us-east-1:467586748896:stateMachine:DealerCount"
                    })
                }
            }, (err)=> {
                if (err) { console.error('csv err', err); return err; }
            })
    })
}
