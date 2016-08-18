'use strict';
const debug = require('debug')('CommonDao');
const Metadata = require('./metadata');

class Batch {
  constructor(metadata){
    this.metadata = metadata;
    this.data = []
  }

  update(obj){
    var sql = this.metadata.generateUpdate(obj);
    this.data.push([sql, obj]);
    return this;
  }

  insert(obj){
    var sql = this.metadata.generateInsert(obj);
    this.data.push([sql, obj]);
    return this;
  }

  delete(obj){
    var sql = this.metadata.generateDelete(obj);
    this.data.push([sql, obj]);
    return this;
  }

  scripts(){
    return this.data;
  }

  *execute(){
    return yield this.metadata.batch(this.data);
  }
}

class Dao {
  constructor(table, options){
    this.table = table;
    this.metadata = new Metadata(table, options);
  }

  *batch(){
    yield this.metadata.ensureLoad();
    return new Batch(this.metadata);
  }

  *get(obj) {
    yield this.metadata.ensureLoad();
    var sql = this.metadata.generateSelect(obj, null, null, true);
    debug('get', sql, obj);
    var result = yield this.metadata.execute(sql, obj);
    return result[0];
  }

  *select(obj, options, extra){
    if ( arguments.length == 2 && typeof(options) == 'string') {
      extra = options;
      options = null;
    }
    yield this.metadata.ensureLoad();
    var sql = this.metadata.generateSelect(obj, null, extra);
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

  *execute(sql, params, options){
    var result = yield this.metadata.execute(sql, params, options);
    return result[1];
  }

  connection(){
    return this.metadata.getConnection();
  }
}

module.exports = Dao;
