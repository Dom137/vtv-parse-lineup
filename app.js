const AWS = require('aws-sdk');
const axios = require('axios');

const DEV_MODE = false;

const s3 = new AWS.S3();
const BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const FOLDER_NAME = process.env.AWS_FOLDER_NAME;
const TODAY = new Date().toISOString().split('T')[0]; // Get today's date in YYYY-MM-DD format
//const TODAY = '2024-10-09';
const OPCO_ENT_TYPE = 'opco';
const CHANNEL_ENT_TYPE = 'channel';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
 
const AIOPS_AUTH_EP = process.env.AIOPS_AUTH_EP; 
const AIOPS_AUTH_EP_USER = process.env.AIOPS_AUTH_EP_USER; 
const AIOPS_AUTH_EP_PW =  process.env.AIOPS_AUTH_EP_PW; 
let AIOPS_AUTH_TOKEN = '';

const AIOPS_RESOURCES_EP = process.env.AIOPS_RESOURCES_EP; 
const AIOPS_REFERENCES_EP = process.env.AIOPS_REFERENCES_EP; 

// function to get the Auth token
async function getAuthToken() {
    try {
      const response = await axios.post(
        AIOPS_AUTH_EP,
        {
          username: AIOPS_AUTH_EP_USER,
          api_key: AIOPS_AUTH_EP_PW
        },
        {
          headers: {
            'Content-Type': 'application/json',
          }
        }
      );
  
      // Extract the token from the response data
      const token = response.data.token;
      
      // Return the token
      return token;
    } catch (error) {
      console.error('Error:', error.response ? error.response.data : error.message);
    }
  }

// Function to get list of files from S3
async function listFiles() {
    const params = {
        Bucket: BUCKET_NAME,
        Prefix: `${FOLDER_NAME}/`
    };
    try {
        const data = await s3.listObjectsV2(params).promise();
        return data.Contents.map(item => item.Key);
    } catch (error) {
        console.error('Error listing files:', error);
        throw error;
    }
}

// Function to get file content from S3
async function getFileContent(fileName) {
    const params = {
        Bucket: BUCKET_NAME,
        Key: fileName
    };
    try {
        const data = await s3.getObject(params).promise();
        return JSON.parse(data.Body.toString());
    } catch (error) {
        console.error(`Error reading file ${fileName}:`, error);
        throw error;
    }
}

// Function to filter files based on the current date (in the filename)
function filterFilesByDate(fileKeys, date) {
    return fileKeys.filter(fileKey => {
        const fileName = fileKey.split('/').pop(); // Get file name
        const fileDate = fileName.split('_')[1];   // Extract date part (after the underscore)
        if (fileDate) {
            const fullDate = fileDate.split('.')[0];
            return fullDate === date;
        }
        else {
            return false;
        }

    });
}

// Function to send data to the REST API
async function sendDataToAPI(twoLetterCode, epgData) {
    const headers = {
        'accept': 'application/json',
        'X-TenantID': 'cfd95b7e-3bc7-4006-a4a8-a73a79c71255',
        'JobId': 'vtv-channel-load',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + AIOPS_AUTH_TOKEN
    };
    try {
        // 1. Send the object with the OPCO code
        const postDataOpco = {
            uniqueId: twoLetterCode,
            entityTypes: [OPCO_ENT_TYPE],
            name: twoLetterCode,
            matchTokens: twoLetterCode
        };

        try {
            const response = await axios.post(AIOPS_RESOURCES_EP, postDataOpco, { headers });
            console.log(`Successfully sent data for OPCO ${twoLetterCode}:`, response.status);
        } catch (error) {
            console.log(error);
            console.error(`Error sending data for OPCO ${twoLetterCode}:`, error.response ? error.response.data : error.message);
        }
        // 2. Send each EPG_ID and its attributes
        for (const [EPG_ID, attributes] of Object.entries(epgData)) {
            const channelUniqueId = twoLetterCode + '_' + EPG_ID + '_' + epgData[EPG_ID].name;
            const postDataChannel = {
                uniqueId: channelUniqueId,
                entityTypes: [CHANNEL_ENT_TYPE],
                name: epgData[EPG_ID].name,
                matchTokens: [channelUniqueId, twoLetterCode + '_' + EPG_ID],
                channelType: epgData[EPG_ID].channelType,
                typeDescription: epgData[EPG_ID].typeDescription,
                channelNumber: epgData[EPG_ID].Channel_Number,
                opco: twoLetterCode,
                epgId: EPG_ID,
                tags: [EPG_ID, twoLetterCode,twoLetterCode + '_' + EPG_ID]
            };
            if (!DEV_MODE) {
                try {
                    const response = await axios.post(AIOPS_RESOURCES_EP, postDataChannel, { headers });
                    console.log(`Successfully sent data for channel ${epgData[EPG_ID].name} in OPCO ${twoLetterCode}:`, response.status);
                } catch (error) {
                    console.log(error);
                    console.error(`Error sending data for channel ${epgData[EPG_ID].name} in OPCO ${twoLetterCode}:`, error.response ? error.response.data : error.message);
                }
            }
            else {                
                if(epgData[EPG_ID].name == 'RTL' || epgData[EPG_ID].name == 'DAZN 2 (Sky)'){
                    console.warn('DEBUG-MODE: Only processing data for channels <RTL> and <DAZN 2 (Sky)>!');
                    // console.log(postDataChannel);
                    try {
                        const response = await axios.post(AIOPS_RESOURCES_EP, postDataChannel, { headers });
                        console.log(`Successfully sent data for channel ${epgData[EPG_ID].name} in OPCO ${twoLetterCode}:`, response.status);
                    } catch (error) {
                        console.log(error);
                        console.error(`Error sending data for channel ${epgData[EPG_ID].name} in OPCO ${twoLetterCode}:`, error.response ? error.response.data : error.message);
                    }
                }

            }

            // 3. Create the relation from OPCO to channel
            const postDataRelation = {
                _fromUniqueId: twoLetterCode,
                _toUniqueId: channelUniqueId,
                _edgeType: 'contains'
            }
            if (!DEV_MODE) {
                try {
                    const response = await axios.post(AIOPS_REFERENCES_EP, postDataRelation, { headers });
                    console.log(`Successfully created relation for channel ${channelUniqueId} with OPCO ${twoLetterCode}:`, response.status);
                } catch (error) {
                    console.log(error);
                    console.error(`Error creating relation for channel ${channelUniqueId} with OPCO ${twoLetterCode}!`);
                }
            }
            else {                
                if(epgData[EPG_ID].name == 'RTL'|| epgData[EPG_ID].name == 'DAZN 2 (Sky)'){
                    console.warn('DEBUG-MODE: Only processing relation data for channels <RTL> and <DAZN 2 (Sky)>!');
                    try {
                        const response = await axios.post(AIOPS_REFERENCES_EP, postDataRelation, { headers });
                        console.log(`Successfully created relation for channel ${channelUniqueId} with OPCO ${twoLetterCode}:`, response.status);
                    } catch (error) {
                        console.log(error);
                        console.error(`Error creating relation for channel ${channelUniqueId} with OPCO ${twoLetterCode}!`);
                    }
                }

            }

        }
    } catch (error) {
        console.error(`Error sending data for code: ${twoLetterCode}`, error);
    }
}

// Function to extract two-letter code from filename and EPG_ID from content
async function processFiles() {
    try {
        const fileKeys = await listFiles();
        const todayFileKeys = filterFilesByDate(fileKeys, TODAY); // Filter by today's date
        const result = {};

        for (const fileKey of todayFileKeys) {
            const fileName = fileKey.split('/').pop();           // Get file name
            const twoLetterCode = fileName.slice(0, 2);          // Extract first two letters
            if (twoLetterCode) {
                console.log(`Processing file: ${fileName}, extracted code: ${twoLetterCode}`);
                const fileContent = await getFileContent(fileKey);

                if (!result[twoLetterCode]) {
                    result[twoLetterCode] = {};  // Initialize if not already set
                }

                fileContent.forEach(entry => {
                    const { EPG_ID, ...attributes } = entry;
                    result[twoLetterCode][EPG_ID] = { ...attributes };  // Set the EPG_ID as the key and entry attributes as value
                });
            }
        }

        return result;
    } catch (error) {
        console.error('Error processing files:', error);
        throw error;
    }
}

// Main function
(async () => {
    AIOPS_AUTH_TOKEN = await getAuthToken();
    console.log('Looking for line up files with date:', TODAY);
    const lineupData = await processFiles();
    //console.log('Lineup Data:', lineupData['DE'][7159]);
    // Send data to REST API for each twoLetterCode and its associated EPG_IDs
    for (const [twoLetterCode, epgData] of Object.entries(lineupData)) {
        await sendDataToAPI(twoLetterCode, epgData);
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
})();
