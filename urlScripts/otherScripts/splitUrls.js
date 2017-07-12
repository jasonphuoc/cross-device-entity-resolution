let lineReader = require('line-reader'),
sleep = require('sleep'),
file = require("fs");

let count = 0;
process.stdout.write("Lines completed:")
lineReader.eachLine(process.argv[2], function(line, last) {
  let lineSplit = line.split(',');
  let fid = lineSplit[0];
  let urlSplit = lineSplit[1].split('/');
  let currUrl = '';
  for(let i=0; i<urlSplit.length; i++) {
    if(i == 0)
      currUrl = urlSplit[0]
    else
      currUrl  = currUrl + '/' + urlSplit[i];

    file.appendFileSync("./data/splitUrls.csv", fid + ',' + currUrl + '\n');
  }
  ++count
  if(count % 1000 == 0) process.stdout.write(count + '...');
  if (last == true) {
    sleep.sleep(1);
    console.log('\n' + process.argv[2] + ' processing complete');
    process.exit();
  }
});
