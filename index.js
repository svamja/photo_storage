
require('dotenv').config();

const MongodbModel = require('nodejs-mongodb-model');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');
const https = require('https');

const PhotoStorage = {

    /*
        Complete workflow
        
        * create bucket on google storage
        * create .env file with BUCKET_NAME=<your-bucket-name> at root of this project
        * (optionally) updat .env file with BUCKET_PREFIX to provide folder path. default = photos.
        * create google console credentials (Create Credentials > OAuth > Desktop Application)
            permission to read Google Photos Library
        * download credentials file as ".google_credentials.json"
        * create google service account and key (Create Credentials > Service Account)
            permission to upload to Cloud Storage
        * download and save the key as ".google_service_key.json"
        * run backup() - use below command to execute:
            npx run-method index backup
          This will copy photos from google photos to google storage
            and update database.
          It will time out after 1 minute. You can run it for longer by passing number
          of minutes, eg:
            npx run-method index backup 10
        * keep running it until it catches up
        * run it every day / week to backup up new photos

    */

    options: {
        credPath: __dirname + '/.google_credentials.json',
        serviceKeyPath: __dirname + '/.google_service_key.json',
        bucketName: process.env.BUCKET_NAME,
        bucketPath: process.env.BUCKET_PREFIX || '',
        mongodbUrl: process.env.MONGODB_URL || 'mongodb://localhost',
        mongodbDbName: process.env.MONGODB_DB_NAME || 'photo_storage',
        tmpPath: __dirname + '/tmp',
        folderStyle: process.env.FOLDER_STYLE || 'monthly',
    },

    async getGoogleClient() {
        if(this.client) {
            return this.client;
        }
        const GoogleAuth = require('../browser-google-auth');
        GoogleAuth.setOptions({
            credPath: this.options.credPath,
            scope: [ 'https://www.googleapis.com/auth/photoslibrary.readonly' ]
        });
        this.client = await GoogleAuth.getClient();
        return this.client;
    },

    sleep: function(s) {
      return new Promise(resolve => setTimeout(resolve, s*1000));
    },

    async startTimedCounts(interval = 10) {
        this.isTimedCountsActive = true;
        for(let i = 0; i < 10000; i++) {
            if(!this.isTimedCountsActive) {
                break;
            }
            console.log(new Date(), this.counts);
            await this.sleep(interval);
        }
    },

    stopTimedCounts() {
        this.isTimedCountsActive = false;
    },

    async backup(minutes = 1) {

        MongodbModel.init(this.options.mongodbUrl, this.options.mongodbDbName);

        this.StorageFiles = await MongodbModel.model('StorageFiles');
        this.PhotosFiles = await MongodbModel.model('PhotosFiles');

        this.counts = { pages: 0, in: 0, new: 0, present: 0, out: 0 };

        this.startTimedCounts(10); // Report counts every 10 seconds

        let pageToken;
        let pages = 0;
        minutes = parseFloat(minutes);
        this.expiry = new Date().getTime() + minutes*60000;

        while(true) {
            this.counts.pages++;
            response = await this.getPhotosByPage(pageToken);
            items = response.mediaItems;
            await this.backupChunk(items);
            pageToken = response.nextPageToken;
            if(!pageToken) {
                break;
            }
            if(new Date().getTime() >= this.expiry) {
                break;
            }
        }

        this.stopTimedCounts();
        await MongodbModel.close();
        return this.counts;

    },

    async getPhotosByPage(pageToken) {
        const client = await this.getGoogleClient();
        const url = 'https://photoslibrary.googleapis.com/v1/mediaItems';
        const pageSize = 100;
        const params = { pageSize, pageToken };
        const response = await client.request({ url, params });
        await this.sleep(5); // maintain API rate
        return response.data;
    },

    async backupChunk(photos) {
        for(let item of photos) {
            this.counts.in++;
            // Insert into database if not present
            let photo = await this.PhotosFiles.findOne({ filename: item.filename });
            if(!photo) {
                await this.PhotosFiles.insertOne(item);
                this.counts.new++;
                photo = await this.PhotosFiles.findOne({ filename: item.filename });
            }
            if(photo.storage && photo.storage.path) {
                this.counts.present--;
                continue;
            }
            await this.backupPhoto(photo);
            if(new Date().getTime() >= this.expiry) {
                break;
            }
        }
    },

    async backupPhoto(photo) {

        // Initialize
        const localPath = this.options.tmpPath + '/' + photo.filename;
        const bucket = this.options.bucketName;
        const destination = this.getTargetPath(photo);
        console.log(destination)

        // Storage Client
        if(!this.storage) {
            const keyFilename = this.options.serviceKeyPath;
            this.storage = new Storage({ keyFilename });
        }

        // Download Photo
        const photo_url = photo.baseUrl + '=d';
        await this.downloadUrl(photo_url, localPath);
        await this.sleep(3); // maintain download rate

        // Verify Download
        const fileStats = fs.statSync(localPath);
        if(fileStats.size < 100) {
            throw new Error('unable to download ' + photo.filename);
        }

        // Upload to Bucket
        await this.storage.bucket(bucket).upload(localPath, { destination });

        // Update Database
        await this.PhotosFiles.updateOne(
            { _id: photo._id },
            { '$set': { storage: { uploaded: new Date().getTime(), path: destination } } 
        });
        this.counts.out++;
    },

    getTargetPath(photo) {
        consteationTime = photo.mediaMetadata.creationTime;
        const year = creationTime.substring(0, 4);
        const month = creationTime.substring(5, 7);
        const bucket = this.options.bucketName;
        let targetPath;
        if(this.options.folderStyle == 'monthly') {
            targetPath = this.options.bucketPath + year + '/' + month + '/' + photo.filename;
        }
        else if(this.options.folderStyle == 'yearly') {
            targetPath = this.options.bucketPath + year + '/' + photo.filename;
        }
        else {
            targetPath = this.options.bucketPath + photo.filename;
        }
        return targetPath;
    },

    async downloadUrl(url, localPath) {
        return new Promise(function(resolve, reject) {
            const file = fs.createWriteStream(localPath);
            const request = https.get(url, function(response) {
                if (response.statusCode < 200 || response.statusCode >= 300) {
                    return reject(new Error('statusCode=' + response.statusCode));
                }
                response.pipe(file);
                response.on('end', function() {
                    resolve();
                });
            })
            .on('error', function(error) {
                reject(error);
            });
            request.end();
        });
    },

};

if(!module.parent) {
    (async function() {
        await PhotoStorage.backup();
    })();
}

module.exports = PhotoStorage;

