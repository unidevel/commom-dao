'use strict';

class Metadata {
  constructor(table, options){
    if ( !options || !options.adapter ) {
      throw new Error('Required parameter adapter is missing!');
    }
    if ( !table ) {
      throw new Error('Table name not provide!');
    }
    this.table = table;
    this.adapter = options.adapter;
  }

  *ensureLoad(){
    yield this.adapter.ensureLoad();
  }

  *execute(sql, params, options){
    return yield this.adapter.execute(sql, params, options)
  }

	generateWhere(args, primaryKeyOnly, keys){
		var cols = [];
		if ( !keys ) keys = [];
		for ( var col in args ) {
			if ( this.adapter.isPrimaryKey(col) ) {
				cols.push(col);
			}
			else if ( !primaryKeyOnly && this.adapter.isIndex(col) ){
				cols.push(col);
			}
			else {
				if ( this.adapter.exists(col) )
					keys.push(col);
			}
		}
		var sql = '';
		var first = true;
		for ( var i = 0; i < cols.length; ++ i ) {
			var col = cols[i];
			//sql += cols[i] +'=:'+cols[i];
			var pair = this.adapter.wherePair(col, args[col]);
			if ( sql ) sql += ' and ';
			sql += pair;
		}
		return sql;
	}

	generateSelect(args, cols, extra){
		var where = this.generateWhere(args);
		var colStr = '';
		colStr = this.adapter.selectColumns(cols);
		var sql = 'select '+ colStr +' from '+this.table ;
		if ( where ) sql +=' where '+where;
    if ( extra ) sql += extra;
		return sql;
	}

	generateInsert(args){
		var cols = [];
    var hasPrimaryKey = false;
		for(var key in args){
			if ( this.adapter.exists(key) ) cols.push(key);
      if ( this.adapter.isPrimaryKey(key) ) hasPrimaryKey = true;
		}
    if ( !hasPrimaryKey ) throw new Error('No primary key in insert data!');
		var sql = 'insert into '+this.table + '('+this.adapter.selectColumns(cols)+') values (';
		sql += ':'+cols.join(',:');
		sql += ')';
		return sql;
	}

	generateUpdate(args){
		var cols = [];
		var sql = 'update '+this.table + ' set ';
		var where = this.generateWhere(args, true, cols);
    sql += this.adapter.columnsPair(cols);
		sql += ' where '+where;
		return sql;
	}

	generateDelete(args){
		var sql = 'delete from '+this.table+' where '+this.generateWhere(args, true);
		return sql;
	}
}

module.exports = Metadata;
