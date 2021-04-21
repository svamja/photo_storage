# Photo Storage

one-way backup from Google Photos to Google Cloud Storage

# Workflow

* Create bucket on google storage
* Create .env file with BUCKET_NAME=<your-bucket-name> at root of this project
* (optionally) updat .env file with BUCKET_PREFIX to provide folder path. default = photos.
* Create google console credentials (Create Credentials > OAuth > Desktop Application)
    permission to read Google Photos Library
* Download credentials file as ".google_credentials.json"
* Create google service account and key (Create Credentials > Service Account)
    permission to upload to Cloud Storage
* Download and save the key as ".google_service_key.json"
* Run backup - use below command to execute:

    npx run-method index backup

  This will ask for permissions.

  Then it will copy photos from google photos to google storage
    and update database.

  It will time out after 1 minute. You can run it for longer by passing number
  of minutes, eg:

    npx run-method index backup 10

* Keep running it until it catches up
* Run it every day / week / month to backup up new photos



