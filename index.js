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
    //Call Process Document here : ==> document will be name-ofuploadedfile
    res.send("File Uploaded")
})

app.post("/getResult", async (req, res) => {
    //TODO
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

const PORT = process.env.PORT||3000 
app.listen(PORT, () => {
    console.log(`Running on port:: ${PORT}`)
})

