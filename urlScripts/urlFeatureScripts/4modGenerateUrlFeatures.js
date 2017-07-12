let async = require('async'),
redis = require('redis'),
scripto = require('redis-scripto'),
conn = redis.createClient({port: 6379}),
file = require("fs");

let count = 0;
let scriptManager = new scripto(conn);
scriptManager.loadFromFile('generateUrlFeatures', './../lua/generateUrlFeatures.lua');
let contents = file.readFileSync(process.argv[2], 'utf8');
let contentArray = contents.split('\n');
contentArray.pop();
process.stdout.write("Lines Completed:")

async.forEachLimit(contentArray, 1, function(line, callback) {
  let lineSplit = line.split(',');
  let uid1 = lineSplit[0];
  let uid2 = lineSplit[1];
  scriptManager.run('generateUrlFeatures', [uid1], [uid2, 'MODELING'], function(err, result) {
    if(err) throw err;
    for(let i=0; i<5; i++)
      file.appendFileSync("./../features/pairs_for_modeling_url_l" + (i+1) + ".csv", uid1 + "," + uid2 + "," + result[i] + '\n');
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
