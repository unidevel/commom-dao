'use strict';
var Dao = require('./lib/dao');
var Metadata = require('./lib/metadata');
module.exports = function createCommonDao(table, options){
  return new Dao(table, options);
}
