'use strict';

module.exports.getDealerData = (event, context, callback) => {
    const ftp = require("basic-ftp")
    const fs = require("fs")
    const csvtojsonV2 = require("csvtojson");
    const AWS = require("aws-sdk")
    const S3 = new AWS.S3()
    const process = require('process');
    const async = require('async')
    let ftpHost;
    let ftpUsername;
    let ftpPassword;

    if(event.iterator && event.iterator.index){
        console.log(`Restarting execution at ${event.iterator.index}`)
        callback(null, {
            index: event.iterator.index,
            runCount: event.iterator.runCount,
        })
    }else{
        getCredentials().then(response =>{
            downloadList().then((success, err)=>{
                let dataParams = {
                    Bucket: 'nightlydealerdata',
                    Key: 'Hunterdouglas_DealerLocator.txt',
                    Body: fs.readFileSync("/tmp/Hunterdouglas_DealerLocator.txt")
                }
                console.log('Talking to S3')
                S3.putObject(dataParams, (err,data)=>{
                    if(err){
                        console.log('err', err)
                        return err
                    }
                    console.log('File Saved to S3')
                    csvtojsonV2({delimiter: "\t"})
                        .fromFile('/tmp/Hunterdouglas_DealerLocator.txt')
                        .then((jsonObj) =>{
                            console.log(jsonObj.length,' dealers')
                            callback(null, {
                                index: 0,
                                runCount: jsonObj.length-1
                            })
                        })
                })
            })
        })
    }
    async function getCredentials(){
        const encrypted = [process.env['FTPUSERNAME'], process.env['FTPPASSWORD'], process.env['FTPHOST']];
        try {
            await async.eachSeries(encrypted, (value, cb)=>{
                const kms = new AWS.KMS();
                kms.decrypt({ CiphertextBlob: Buffer.from(value, 'base64') }, (err, data) => {
                    if (err) {
                        console.log('Decrypt error:', err);
                        cb()
                        return callback(err);
                    }
                    if(value === process.env['FTPUSERNAME']){
                        ftpUsername = data.Plaintext.toString('ascii');
                    }
                    if(value === process.env['FTPPASSWORD']){
                        ftpPassword = data.Plaintext.toString('ascii');
                    }
                    if(value === process.env['FTPHOST']){
                        ftpHost = data.Plaintext.toString('ascii');
                    }
                    cb()
                });
            })
        }
        catch (e) {
            console.log('e', e)
        }
    }
    async function downloadList() {
        const client = new ftp.Client();
        try {
            await client.access({
                host: ftpHost,
                user: ftpUsername,
                password: ftpPassword,
                secure: false
            })
            await client.download(fs.createWriteStream("/tmp/Hunterdouglas_DealerLocator.txt"), "Hunterdouglas_DealerLocator.txt")
            console.log('FTP File Downloaded')
        }
        catch(err) {
            console.log(err)
        }
        console.log('FTP closing')
        client.close();
    }
}
