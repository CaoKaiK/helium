import * as functions from "firebase-functions";
import admin = require("firebase-admin");

admin.initializeApp();

// // Start writing Firebase Functions
// // https://firebase.google.com/docs/functions/typescript
//
exports.scheduledFunctionCrontab = functions.pubsub
    .schedule("* * * * *")
    .timeZone("America/New_York")
    .onRun(() => {
      console.log("This will be run every day at 11:05 AM Eastern!");
      return null;
    });
