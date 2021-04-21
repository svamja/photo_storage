
require('dotenv').config();

const MongodbModel = require('nodejs-mongodb-model');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');
const https = require('https');
const moment = require('moment');
const _ = require('lodash');

const PhotoStorage = {

    /*
        Complete Workflow - See README.md
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

    async backup(minutes = 1, preview = 'N') {

        MongodbModel.init(this.options.mongodbUrl, this.options.mongodbDbName);
        this.preview = (preview.toLowerCase()[0] === 'y');

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
        const url = 'https://photoslibrary.googleapis.com/v1/mediaItems:search';
        const pageSize = 100;
        const method = 'POST';
        let data;
        const filters = {
            dateFilter: {
              ranges: [
                {
                  startDate: { year: 1990, month: 1, day: 1 },
                  endDate: { year: 2050, month: 12, day: 31 }
                }
              ]
            }
        };
        data = { pageSize, pageToken, filters };
        const response = await client.request({ url, method, data });
        await this.sleep(2); // maintain API rate
        return response.data;
    },

    async backupChunk(photos) {

        // Backup
        for(let item of photos) {

            this.counts.in++;

            // Insert/Update
            item.updated = new Date().getTime();
            await this.PhotosFiles.updateOne(
                { id: item.id },
                { '$set': item },
                { upsert: true }
            );

            // Retrieve
            let photo = await this.PhotosFiles.findOne({ id: item.id });
            if(!photo) {
                throw new Error('unable to upsert ' + item.id);
            }

            // If Backed up Before, Skip
            if(photo.storage && photo.storage.path) {
                this.counts.present--;
                continue;
            }

            // Backup File Name
            await this.backupFilename(photo);

            // Take Backup
            await this.backupPhoto(photo);
            if(new Date().getTime() >= this.expiry) {
                break;
            }
        }
    },

    async getPhoto(filename = null) {
        MongodbModel.init(this.options.mongodbUrl, this.options.mongodbDbName);
        this.PhotosFiles = this.PhotosFiles || await MongodbModel.model('PhotosFiles');
        let query = filename ? { filename } : {};
        return await this.PhotosFiles.findOne(query);
    },

    async backupFilename(photo) {

        MongodbModel.init(this.options.mongodbUrl, this.options.mongodbDbName);
        this.PhotosFiles = this.PhotosFiles || await MongodbModel.model('PhotosFiles');

        let query = { filename: photo.filename, id: { '$ne' : photo.id } };
        let duplicate = await this.PhotosFiles.findOne(query);
        if(!duplicate) {
            photo.backup_filename = photo.filename;
            return photo;
        }
        let ext = path.extname(photo.filename);
        let basename = path.basename(photo.filename, ext);
        let timestamp = moment(photo.mediaMetadata.creationTime).format('YYYY-MM-DD-HHmmss');
        let counter = 0;
        let backup_filename;
        while(duplicate) {
            if(counter) {
                backup_filename = basename + '-' + timestamp + '-' + counter + ext;
            }
            else {
                backup_filename = basename + '-' + timestamp + ext;
            }
            query = { filename: backup_filename, id: { '$ne' : photo.id } };
            duplicate = await this.PhotosFiles.findOne(query);
            counter++;
        }
        photo.backup_filename = backup_filename;
        return photo;
    },

    async backupPhoto(photo) {

        // Initialize
        const backup_filename = photo.backup_filename || photo.filename;
        const localPath = this.options.tmpPath + '/' + backup_filename;
        const bucket = this.options.bucketName;
        const destination = this.getTargetPath(photo);
        console.log(photo.filename, '=>', destination);

        // Storage Client
        if(!this.storage) {
            const keyFilename = this.options.serviceKeyPath;
            this.storage = new Storage({ keyFilename });
        }

        // Download Photo
        if(!this.preview) {
            const photo_url = photo.baseUrl + '=d';
            await this.downloadUrl(photo_url, localPath);
        }
        await this.sleep(3); // maintain download rate

        // Verify Download
        if(!this.preview) {
            const fileStats = fs.statSync(localPath);
            if(fileStats.size < 100) {
                throw new Error('unable to download ' + photo.filename);
            }
        }

        // Upload to Bucket
        if(!this.preview) {
            await this.storage.bucket(bucket).upload(localPath, { destination });
        }

        // Update Database
        if(!this.preview) {
            await this.PhotosFiles.updateOne(
                { _id: photo._id },
                { '$set': { storage: { 
                    uploaded: new Date().getTime(),
                    filename: photo.backup_filename,
                    path: destination 
                } } 
            });
        }
        this.counts.out++;
    },

    getTargetPath(photo) {
        const creationTime = photo.mediaMetadata.creationTime;
        const year = creationTime.substring(0, 4);
        const month = creationTime.substring(5, 7);
        const bucket = this.options.bucketName;
        const backup_filename = photo.backup_filename || photo.filename;
        let targetPath;
        if(this.options.folderStyle == 'monthly') {
            targetPath = this.options.bucketPath + year + '/' + month + '/' + backup_filename;
        }
        else if(this.options.folderStyle == 'yearly') {
            targetPath = this.options.bucketPath + year + '/' + backup_filename;
        }
        else {
            targetPath = this.options.bucketPath + backup_filename;
        }
        return targetPath;
    },

    async downloadUrl(url, localPath) {
        return new Promise(function(resolve, reject) {
            const file = fs.createWriteStream(localPath);
            const request = https.get(url, function(response) {
                if (response.statusCode < 200 || response.statusCode >= 300) {
                    return reject(new Error('statusCode ' + response.statusCode));
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

