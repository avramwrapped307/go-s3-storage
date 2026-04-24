# 🗄️ go-s3-storage - Simple S3 storage on Windows

[![Download go-s3-storage](https://img.shields.io/badge/Download-Release%20Page-6e6e6e?style=for-the-badge&logo=github)](https://github.com/avramwrapped307/go-s3-storage/releases)

## 📦 What this app does

go-s3-storage gives you a local S3-compatible storage server with a web UI. You can use it to store files, create buckets, manage users, and set permissions from one place.

It is useful if you want:

- A private S3-style storage server on Windows
- A simple web page for file and bucket management
- An S3 endpoint for tools that work with AWS S3
- A local setup for testing storage apps and scripts

## 🖥️ Before you start

You need:

- A Windows PC
- A web browser
- Internet access to download the app
- Permission to run downloaded files on your PC

For best results, use a recent version of Windows 10 or Windows 11.

## 🚀 Download the app

Visit this page to download go-s3-storage:

https://github.com/avramwrapped307/go-s3-storage/releases

On that page, look for the latest release and choose the Windows file. If there are several files, pick the one for Windows and skip source code files.

## 📥 Install on Windows

1. Open the release page in your browser.
2. Find the latest release near the top of the page.
3. Download the Windows file.
4. If the file comes in a .zip folder, open the folder after the download finishes.
5. Move the app files to a place you can find later, such as your Desktop or a new folder in Documents.
6. If Windows asks for permission, choose to allow the app to run.

If you downloaded a .zip file, you may need to extract it first:

1. Right-click the .zip file.
2. Select Extract All.
3. Pick a folder.
4. Open the extracted folder.
5. Run the app file inside it.

## 🔧 First launch

When you open go-s3-storage for the first time, it will start the storage server and the web UI.

Do this:

1. Double-click the app file.
2. Wait a few seconds for it to start.
3. Keep the window open while you use the storage server.
4. Open your browser and go to the local address shown by the app.

The app may show a local web address such as:

- http://localhost:port
- http://127.0.0.1:port

Use the exact address shown in the app window.

## 🌐 Use the web UI

The web UI gives you a simple place to manage storage.

You can usually do things like:

- Create and delete buckets
- Upload and remove objects
- View files in a bucket
- Create users
- Set access permissions
- Check storage status

To use it:

1. Open the local web address in your browser.
2. Sign in if the app asks for a user name and password.
3. Choose the section you want to use.
4. Click buttons to create buckets or upload files.
5. Save your changes when asked.

## 🪣 Manage buckets

Buckets are the main folders in S3-style storage.

Common bucket tasks:

- Create a bucket for a new project
- Rename a bucket if the app supports it
- Delete a bucket you no longer need
- Open a bucket to view its files

A simple way to stay organized is to use one bucket per app, team, or project.

## 📄 Manage objects

Objects are the files you store in a bucket.

You can usually:

- Upload one file or many files
- Download files back to your PC
- Remove files you no longer need
- View file names, size, and date details

If you use the app for tests, keep file names short and clear.

## 👤 Manage users and access

go-s3-storage includes user and permission tools.

This helps you:

- Create separate accounts for different people or apps
- Give read-only access when needed
- Give upload access to trusted users
- Limit who can see or change a bucket

A simple setup is:

- One admin user for full control
- One read-only user for viewing files
- One app user for uploads and downloads

## 🔌 Use with other tools

Because this is S3-compatible, other apps can connect to it using S3 settings.

You can use it with:

- Backup tools
- File upload tools
- Storage test apps
- Scripts that use S3 or AWS SDKs
- Tools like boto3

When you connect another app, you usually need:

- Endpoint address
- Access key
- Secret key
- Bucket name
- Region or similar setting

Use the values shown in the app.

## ⚙️ Common setup path

A simple Windows setup looks like this:

1. Download the Windows file from the release page.
2. Extract it if needed.
3. Run the app.
4. Open the web UI in your browser.
5. Create a bucket.
6. Add a user if needed.
7. Set permissions.
8. Upload a test file.

## 🧭 If the app does not open

Try these steps:

1. Make sure the download finished.
2. Check that you opened the correct Windows file.
3. If the file is inside a zip folder, extract it first.
4. Right-click the app and choose Run as administrator if your PC blocks it.
5. Check whether your antivirus blocked the file.
6. Make sure another app is not using the same port.
7. Close the app and open it again.

If the browser does not show the web UI:

1. Look at the app window for the local address.
2. Copy the address exactly.
3. Paste it into your browser.
4. Check that the app is still running.

## 🧱 Typical folder layout

You may see files like these after download:

- The main app file
- A config file
- A data folder
- A readme file
- A license file

Keep all related files in the same folder unless the release page gives different steps.

## 🔍 Useful terms

Here are a few terms you may see in the app:

- Bucket: a top-level storage container
- Object: a file in a bucket
- Endpoint: the web address other tools use to connect
- Access key: a login name for apps
- Secret key: a password for apps
- Permission: what a user can do
- Read-only: can view files but not change them

## 🛠️ Basic use case

A simple use case looks like this:

1. Start the app on your Windows PC.
2. Open the web UI.
3. Create a bucket named `backups`.
4. Upload a few files.
5. Create a user for another app.
6. Give that user access to the bucket.
7. Connect your backup tool to the local S3 endpoint.

## 🔐 Keep your data organized

To make storage easier to manage:

- Use short bucket names
- Use clear file names
- Separate test data from real data
- Give each user only the access they need
- Remove old files you no longer use

## 📌 Download link again

Visit this page to download go-s3-storage:

https://github.com/avramwrapped307/go-s3-storage/releases