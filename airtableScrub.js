'use strict';
const AWS = require("aws-sdk")
const process = require('process');
const Airtable = require('airtable');
let decrypted;
const encrypted = process.env['AIRTABLEAPIKEY'];

module.exports.scrubAirtable = (event, context, callback) =>{
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
    Airtable.configure({
        endpointUrl: 'https://api.airtable.com',
        apiKey: decrypted
    });
    const base = Airtable.base('app4qgZtb5X0BGxkW');

    base('Dealers (Updated Daily)').select({
        // Selecting the first 3 records in View All:
        view: "To Be Deleted",
        pageSize: 10
    }).eachPage(function page(records, fetchNextPage) {
        // This function (`page`) will get called for each page of records.
        let idsToBeDeleted = [];
        records.forEach(function(record) {
            console.log('Retrieved', record.getId());
            idsToBeDeleted.push(record.getId())
        });
        console.log('idsToBeDeleted', idsToBeDeleted)
        base('Dealers (Updated Daily)').destroy(idsToBeDeleted, function(err, deletedRecords) {
            if (err) {console.error(err);return;}
            console.log('Deleted', deletedRecords.length, 'records');
            // To fetch the next page of records, call `fetchNextPage`.
            // If there are more records, `page` will get called again.
            // If there are no more records, `done` will get called.
            fetchNextPage();
        });
    }, function done(err) {
        if (err) { console.error(err); return; }
        console.log('done')
        callback(null, {
            done: true
        })
    });
}
