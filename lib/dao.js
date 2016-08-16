'use strict';
const debug = require('debug')('CommonDao');
const Metadata = require('./metadata');

class Dao {
  constructor(table, options){
    this.metadata = new Metadata(table, options);
  }

  *select(obj, options){
    yield this.metadata.ensureLoad();
    var sql = this.metadata.generateSelect(obj);
    debug('select', sql, obj);
    var result = yield this.metadata.execute(sql, obj, options);
    return result[0];
  }

  *update(obj, options){
    yield this.metadata.ensureLoad();
    var sql = this.metadata.generateUpdate(obj);
    debug('update', sql, obj);
    var result = yield this.metadata.execute(sql, obj, options);
    return result[0];
  }

  *insert(obj, options){
    yield this.metadata.ensureLoad();
    var sql = this.metadata.generateInsert(obj);
    debug('insert', sql, obj);
    var result = yield this.metadata.execute(sql, obj, options);
    return result[0];
  }

  *delete(obj, options){
    yield this.metadata.ensureLoad();
    var sql = this.metadata.generateDelete(obj);
    debug('delete', sql, obj);
    var result = yield this.metadata.execute(sql, obj, options);
    return result[0];
  }
}

module.exports = Dao;
