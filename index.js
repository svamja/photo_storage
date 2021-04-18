
require('dotenv').config();

const MongodbModel = require('../../nodejs-mongodb-model');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
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
        console.log(this.options);

        MongodbModel.init(this.options.mongodbUrl, this.options.mongodbDbName);
        this.counts = { pages: 0, in: 0, out: 0 };

        this.StorageFiles = await MongodbModel.model('StorageFiles');
        let item = {
            name: 'abc.jpg'
        };
        await this.StorageFiles.insertOne(item);
        await MongodbModel.close();
        return this.counts;

    },

    async indexPhotos() {

        MongodbModel.init(this.options.mongodbUrl, this.options.mongodbDbName);

        this.PhotosFiles = await MongodbModel.model('PhotosFiles');
        await this.PhotosFiles.deleteMany();

        this.counts = { pages: 0, in: 0, out: 0 };

        this.startTimedCounts();

        let pageToken;
        let pages = 0;

        while(true) {
            this.counts.pages++;
            response = await this.listPhotos(pageToken);
            items = response.mediaItems;
            this.counts.in += items.length;
            await this.PhotosFiles.insertMany(items);
            this.counts.out += items.length;
            if(items[0].mediaMetadata && items[0].mediaMetadata.creationTime) {
                this.counts.lastCreated = items[0].mediaMetadata && items[0].mediaMetadata.creationTime;
            }
            pageToken = response.nextPageToken;
            if(!pageToken) {
                break;
            }
            break; //debug
        }

        this.stopTimedCounts();
        console.log(this.counts);
        await MongodbModel.close();

        return this.counts;

    },

    async listPhotos(pageToken) {
        const client = await this.getGoogleClient();
        const url = 'https://photoslibrary.googleapis.com/v1/mediaItems';
        const pageSize = 100;
        const params = { pageSize, pageToken };
        const response = await client.request({ url, params });
        await this.sleep(5);
        return response.data;
    },

    // WIP
    async backup() {

        const Photos = await MongodbModel.model('Photos');
        const photos = await Photos.find();
        this.counts = { in: 0, previous: 0, out: 0 };
        this.startTimedCounts();
        for await(let photo of photos) {
            if(photo.uploaded) {
                this.counts.previous++;
                continue;
            }
            this.counts.in++;
            console.log(photo.filename);
            await this.backupPhoto(photo);
            await Photos.updateOne({ _id: photo._id }, { '$set': { uploaded: true } });
            this.counts.out++;
            break; // debug
        }
        await MongodbModel.close();
        this.stopTimedCounts();
        return this.counts;
    },

    async backupPhoto(photo) {

        // Initialize
        const localPath = photo.filename;
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
        console.log('downloading', photo.filename, creationTime);
        const photo_url = photo.baseUrl + '=d';
        const file = fs.createWriteStream(localPath);
        const request = https.get(photo_url, response => response.pipe(file));

        // Upload to Bucket
        await this.storage.bucket(bucket).upload(localPath, { destination });
        console.log(`uploaded`);

    }

};

if(!module.parent) {
    (async function() {
        await PhotoStorage.indexStorage();
        await PhotoStorage.indexPhotos();
        await PhotoStorage.backup();
    })();
}

module.exports = PhotoStorage;

