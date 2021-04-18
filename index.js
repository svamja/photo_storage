
require('dotenv').config();

const MongodbModel = require('nodejs-mongodb-model');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');
const https = require('https');

const PhotoStorage = {

    /*
        Complete workflow
        
        *  create bucket on google storage
        *  create .env file with BUCKET_NAME=<your-bucket-name> at root of this project
        1. create google console credentials (Create Credentials > OAuth > Desktop Application)
        2. download credentials file as ".google_credentials.json"
        3. create google service account and key (Create Credentials > Service Account)
        4. download and save the key as ".google_service_key.json"
        5. run indexStorage()
            use below command to execute:
                npx run-method index indexStorage
            this will index your google storage backed up photos to mongodb collection 
            named "storage_files" under database "photo_storage".
        6. run indexPhotos()
            use below command to execute:
                npx run-method index indexPhotos
            this will index your google photos to mongodb collection 
            named "photos_files" under database "photo_storage".
        7. run backup()
            use below command to execute:
                npx run-method index backup
            this will copy photos from google photos to google storage
            and updates database.
        8. rerun indexPhotos() and backup() every few days
            to continue backing up

    */

    options: {
        credPath: __dirname + '/.google_credentials.json',
        serviceKeyPath: __dirname + '/.google_service_key.json',
        bucketName: process.env.BUCKET_NAME,
        bucketPath: process.env.BUCKET_PREFIX || '',
        mongodbUrl: process.env.MONGODB_URL || 'mongodb://localhost',
        mongodbDbName: process.env.MONGODB_DB_NAME || 'photo_storage',
        tmpPath: __dirname + '/tmp',
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

    // WIP
    async indexStorage() {

        const moment = require('moment');
        if(!this.storage) {
            const keyFilename = this.options.serviceKeyPath;
            this.storage = new Storage({ keyFilename });
        }

        MongodbModel.init(this.options.mongodbUrl, this.options.mongodbDbName);
        this.StorageFiles = await MongodbModel.model('StorageFiles');
        this.counts = { pages: 0, in: 0, out: 0 };

        const bucketName = this.options.bucketName;
        let options = {};
        if(this.options.bucketPath) {
            options.prefix = this.options.bucketPath;
        }
        const [ files ] = await this.storage.bucket(bucketName).getFiles(options);
        for(let file of files) {
            this.counts.in++;
            let { name, size, md5Hash, crc32c, timeCreated } = file.metadata;
            let created = moment(timeCreated).valueOf();
            let storage_file = {
                name: path.basename(name),
                path: name,
                size: parseInt(size),
                md5Hash,
                crc32c,
                created
            };
            await this.StorageFiles.insertOne(storage_file);
            console.log(storage_file);
            this.counts.out++;
        }
        await MongodbModel.close();
        return this.counts;
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
        console.log('expiry', this.expiry);

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
            let find_photo = await this.PhotosFiles.findOne({ filename: item.filename });
            if(!find_photo) {
                await this.PhotosFiles.insertOne(item);
                this.counts.new++;
            }
            let photo = await this.PhotosFiles.findOne({ filename: item.filename });
            if(photo.storage && photo.storage.path) {
                // Uploaded
                this.counts.present--;
                continue;
            }
            console.log(item.filename, item.mediaMetadata.creationTime);
            await this.backupPhoto(photo);
            if(new Date().getTime() >= this.expiry) {
                break;
            }
        }
    },

    async backupPhoto(photo) {

        // Initialize
        const localPath = this.options.tmpPath + '/' + photo.filename;
        const creationTime = photo.mediaMetadata.creationTime;
        const creationYear = creationTime.substring(0, 4);
        const bucket = this.options.bucketName;
        const destination = this.options.bucketPath + creationYear + '/' + photo.filename;

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

