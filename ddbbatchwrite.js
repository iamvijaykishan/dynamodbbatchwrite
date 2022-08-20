const AWS = require('aws-sdk')
AWS.config.update({
    region: "us-east-1" //Here add you region
});
const s3 = new AWS.S3()
const table = "T_DDBBATCHWRITE";
const docClient = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    var jsonData = '';
    try {
        const params = {
            Bucket: 'bulkupload-s3-ddb', // event.Records[0].s3.bucket.name,
            Key: 'import-2.json' //event.Records[0].s3.object.key
        }
        const file = await s3.getObject(params).promise();
        jsonData = JSON.parse(file.Body.toString('utf-8'))
    } catch (err) {
        console.log(err);
    }

//Batchprocessing begins here
    let batches = []
    const BATCH_SIZE = 25
    while (jsonData.length > 0) {
        batches.push(jsonData.splice(0, BATCH_SIZE))
    }
    console.log(`Total batches: ${batches.length}`)

    let params = '';
    batches.map(processArray)

    async function processArray(item_data) {
        params = {
            RequestItems: {}
        }
        params.RequestItems[table] = []
        item_data.forEach(item => {
            for (let key of Object.keys(item)) {
                if (item[key] === '')
                    delete item[key]
            }
            // Build params
            params.RequestItems[table].push({
                PutRequest: {
                    Item: {
                        ...item
                    }
                }
            })
        })
        batchWritetoDDB(params)
    }
}

async function batchWritetoDDB(params) {
    try {
        console.log('Im about write data to DB' + JSON.stringify(params))
        var results;
        results = await docClient.batchWrite(params).promise();
        console.log('Unprocessed Items  : ' + JSON.stringify(results.UnprocessedItems))
    } catch (e) {
        console.log(e);
    }
    while (Object.keys(results.UnprocessedItems).length > 0) {

        var param = {
            RequestItems: results.UnprocessedItems,
        };
        //results = await docClient.batchWrite(param).promise();
        results = await docClient.batchWrite(param).promise();
        console.log("unprocessed results" + results)
    }
}
