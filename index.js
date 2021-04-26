require('dotenv').config()

//Express 
const express = require("express")
const _ = require("lodash");
const bodyParser = require('body-parser');
const cors = require("cors")
const app = express()

//AWS
const AWS = require("aws-sdk")
const textract = new AWS.Textract();
const sqs = new AWS.SQS();

//S3 
const s3 = new AWS.S3();
const multer = require('multer');
const multerS3 = require('multer-s3');

//Config variables
const Bucket = process.env.Bucket
const document = process.env.Key
const roleArn = process.env.arn
const snsTopicArn = process.env.snsarn
const sqsQueueUrl = process.env.sqsurl


AWS.config.update({
    accessKeyId: process.env.awsAccesskeyID,
    secretAccessKey: process.env.awsSecretAccessKey,
    region: process.env.awsRegion
})


app.use(cors())

app.get("/", (req, res) => {
    res.send("Hey! Welcome to Textract AWS demo using nodeJS")
})

//here we are using multer and multers3 to upload file to s3 bucket for further processing
//accepting file as input param to upload in s3 
app.post("/uploadFile",upload.single('file-to-upload'),async(req,res)=>{
    let response  = await ProcessDocument(Bucket, req.file.originalname);
    res.send(response)
})


let upload = multer({
    storage: multerS3({
        s3:s3,
        bucket:Bucket,
        key:function(req,file,cb){
            console.log("Uploading... "+file)
            cb(null,file.originalname)
        }
    })
})

let ProcessDocument = async (Bucket, document) => {
    await StartDocumentAnalysis(Bucket, document)
    console.log("Analysis Processing: " + startJobId)
    let response=[];
    let jobFound = false
    let dotLine = 0
    let sqsMessage = null
    let sqsParam = { QueueUrl: sqsQueueUrl}
    do {
        sqsMessage = await new Promise((resolve, reject) => {
            sqs.receiveMessage(sqsParam, (err, data) => {
                if (err) {
                    reject(err)
                } else {
                    resolve(data.Messages)
                }
            })
        })
        if (dotLine++ < 40) {
            console.log("...");
        } else {
            dotLine = 0;
        }
        if (sqsMessage != undefined) {
            sqsMessage.forEach(async message => {
                let body = JSON.parse(message.Body)
                let msg = JSON.parse(body.Message)
                let sqsDeleteParam = {
                    QueueUrl: sqsQueueUrl, 
                    ReceiptHandle: message.ReceiptHandle
                }
                if (msg.JobId == startJobId) {
                    if (msg.Status == "SUCCEEDED") {
                        console.log("Getting Data ....")
                        let [resForm,resTable] = await GetDocumentAnalysisResults()
                        response.push(resForm,resTable)
                        jobFound = true
                        sqs.deleteMessage(sqsDeleteParam, (err, data) => {
                            if (err) {
                                console.log("SQSE:" + err)
                            } else {
                                console.log("Deleted from Queue")
                            }
                        })
                    } else {
                        console.log("Document Analysis failed")
                        response.push("Please try again")
                    }
                    //del the msg from queue    
                    sqs.deleteMessage(sqsDeleteParam, (err, data) => {
                        if (err) {
                            console.log("SQSE:" + err)
                        } else {
                            console.log("SQS:" + data)
                        }
                    })
                } else {
                    console.log("JobId not received yet from SQS......")
                    //del the msg from queue   
                    sqs.deleteMessage(sqsDeleteParam, (err, data) => {
                        if (err) {
                            console.log("SQSE:" + err)
                        } else {
                            console.log("SQS:" + data)
                        }
                    })
                }
            });
        } else {
            setTimeout(() => {
                console.log("...")
            }, 10000)
        }
    } while (!jobFound)

    console.log("Finised processing document")
    let params = {Bucket:Bucket,Key:document}
    
    s3.deleteObject(params,(err,data)=>{
        if(err){
            console.log("Trouble Deleting file",err,err.stack)
        }else{
            console.log("Deleted that file from bucket")
        }
    })
    return response
}

//Getting JobId for analaysis operation
let StartDocumentAnalysis = async (Bucket, document) => {
    let params = {
        DocumentLocation: {
            S3Object: {
                Bucket: Bucket,
                Name: document
            }
        },
        FeatureTypes: [
            "FORMS","TABLES",
        ],
        NotificationChannel: {
            RoleArn: roleArn,
            SNSTopicArn: snsTopicArn

        }
    }

    let result = await new Promise((resolve, reject) => {
        textract.startDocumentAnalysis(params, (err, data) => {
            if (err) {
                reject(err)
            } else {
                resolve(data)
            }
        })
    })
    startJobId = result.JobId
}

//getting key value pair and table data
async function GetDocumentAnalysisResults() {
    let maxResults = 1000;
    let paginationToken = null;
    let finished = false;

    let keyMap = {};
    let valueMap = {};
    let blockMap = {};

    let blockMapTable = {};
    let table_blocks = [];


    while (finished == false) {
        let response = null;
        let documentAnalysisRequest = {
            JobId: startJobId,
            MaxResults: maxResults,
            NextToken: paginationToken
        };

        response = await new Promise((resolve, reject) => {
            textract.getDocumentAnalysis(documentAnalysisRequest, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });

        let blocks = response.Blocks;
        console.log("Analyzed Document Text");
        console.log("Pages:" + response.DocumentMetadata.Pages);

    
        blocks.forEach(block => {
            
            blockId = block.Id;
            blockMapTable[blockId] = block;
            blockMap[blockId] = block;

            if (block.BlockType == "KEY_VALUE_SET") {
                if(_.includes(block.EntityTypes,"KEY")){
                    keyMap[blockId] = block
                }else{
                    valueMap[blockId] = block
                }
            }

            if(block.BlockType=="TABLE"){
                table_blocks.push(block)
            }

            //DisplayBlockInfo(block);
            if (response.NextToken) {
                paginationToken = response.NextToken
            } else {
                finished = true;
            }

        });

    }

    //Form data processing 
    let result
    if(keyMap.length <= 0){
        console.log("No Form Data")
    }else{
        const keyValues = getKeyValueRelationship(keyMap, valueMap, blockMap);
        result = keyValues
    }
    
    //Table data processing
    let csv = {}
    if (table_blocks.length <= 0){
        console.log("No tables found")
    }else{
        for(const [index,table] of table_blocks.entries()){
            let [rows,tableId] =  generateTableCsv(table,blockMapTable,index+1)
            csv[tableId] = rows
        }
    }

    console.log("Operation Complete")
    return [result,csv]
    
}

//Generating keyvalue map
let getKeyValueRelationship = (keyMap, valueMap, blockMap) => {

    const keyValues = {};
    const keyMapValues = _.values(keyMap);
    keyMapValues.forEach(keyMapValue => {
        const valueBlock = findValueBlock(keyMapValue, valueMap);
        const key = getText(keyMapValue, blockMap);
        const value = getText(valueBlock, blockMap);
        
        keyValues[key] = value;
    });

    return keyValues;
};

//checking value for key
const findValueBlock = (keyBlock, valueMap) => {
    let valueBlock;
    keyBlock.Relationships.forEach(relationship => {
        if (relationship.Type === "VALUE") {
            relationship.Ids.every(valueId => {
                if (_.has(valueMap, valueId)) {
                    valueBlock = valueMap[valueId];
                    return false;
                }
            });
        }
    });

    return valueBlock;
};

//getting actual data 
const getText = (result, blocksMap) => {
    let text = "";

    if (_.has(result, "Relationships")) {
        result.Relationships.forEach(relationship => {
            if (relationship.Type === "CHILD") {
                relationship.Ids.forEach(childId => {
                    const word = blocksMap[childId];
                    if (word.BlockType === "WORD") {
                        text += `${word.Text} `;
                    }
                    if (word.BlockType === "SELECTION_ELEMENT") {
                        if (word.SelectionStatus === "SELECTED") {
                            text += `X `;
                        }
                    }
                });
            }
        });
    }

    return text.trim();
};


let generateTableCsv = (tableResult,blockMap,tableIndex) => {
    
    let rows = getRowsColumnsMap(tableResult,blockMap)
    let tableId = tableIndex.toString()
    return [rows,tableId]
}

let getRowsColumnsMap = (tableResult,blockMap) =>{
    let rows = {}
    tableResult.Relationships.forEach((relationship)=>{
        if(relationship.Type == "CHILD"){
            
            relationship.Ids.forEach((valueId)=>{
                var cell = blockMap[valueId]
                if(cell.BlockType=="CELL"){
                    let rowIndex = cell.RowIndex
                    let colIndex = cell.ColumnIndex
                    if(!(_.has(rows, rowIndex))){
                        rows[rowIndex] = {}
                    }
                    rows[rowIndex][colIndex] = getText(cell,blockMap)
                }  
            })
        }
    })
    return rows
}



const PORT = process.env.PORT||3000 
app.listen(PORT, () => {
    console.log(`Running on port:: ${PORT}`)
})

