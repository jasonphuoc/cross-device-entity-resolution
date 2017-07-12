let async = require('async')
redis = require("redis"),
scripto = require('redis-scripto'),
conn = redis.createClient({port: 6379}),
lineByLine = require('n-readlines'),
liner = new lineByLine(process.argv[2]);

let count = 0;
let contentArray = [];
let scriptManager = new scripto(conn);
scriptManager.loadFromFile('indexFacts', './../lua/indexFacts.lua');
process.stdout.write("Lines completed:")
let line = "";
while (line = liner.next()) {
  contentArray[count] = line.toString('ascii');
  count++;
}
count = 0;

async.forEachLimit(contentArray, 1, function(line, callback) {
  scriptManager.run('indexFacts', [line], [""], function(err, result) {
    if(err) throw err;
    count++;
    if(count % 100 == 0)
      process.stdout.write(count + '...');
    callback();
  });
}, function(err) {
    if (err) return next(err);
    console.log('\n' + process.argv[2] + ' processing complete');
    process.exit();
});
