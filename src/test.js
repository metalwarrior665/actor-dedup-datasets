const BigMap = require('big-map-simple');

const map = new BigMap();

map.set('a', 1);
map.set('b', 1);

console.dir(map.entries())
