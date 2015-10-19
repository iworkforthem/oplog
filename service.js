var MongoClient = require('mongodb').MongoClient;

var url = 'mongodb://127.0.0.1:27017/local';              // note: change to ip of mongodb.

MongoClient.connect(url, function(err, db) {
  console.log("Connected correctly to: " + url);

  db.collection('oplog.$main', function (err, col) {      // oplog.rs for Replica Set | oplog.$main for Master/Slave Replication

      var stream = col.find({
        ns: 'll_staging.statements',                      // todo: change to my collection
        op: 'i'                                           // i for insert | u for update | d for delete
      },{
        tailable: true,
        awaitData: true,
        noCursorTimeout : true,
        numberOfRetries: Number.MAX_VALUE
      }).stream();

      // Waiting for data
      stream.on('data', function(val) {
        if ( val != null ) {
          console.log('');
          console.log(val);

          // todo: add logic here.

        }

      });

      stream.on('error', function(val) {
         console.log('Error: ', val);
      });
      stream.on('end', function(){
        console.log('End of stream');
      });
  });
});
