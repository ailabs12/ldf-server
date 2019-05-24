let Datasource = require('./Datasource');
let LRU = require('lru-cache');
let { DB } = require('sql-triplestore');

// Creates a new SQLDatasource
function SQLDatasource(options) {
  if (!(this instanceof SQLDatasource))
    return new SQLDatasource(options);
  Datasource.call(this, options);

  // cache
  this._countCache = new LRU({ max: 1000, maxAge: 1000 * 60 * 60 * 3 });
  this._countResults = new LRU({ max: 1000, maxAge: 1000 * 60 * 60 * 3 });

  this._options = options = options || {};
}
Datasource.extend(SQLDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Prepares the datasource for querying
SQLDatasource.prototype._initialize = function (done) {
  this._dbConfig = {
    dialect: this._options.dialect,
    database: this._options.database,
    table: this._options.table,
    host: this._options.url,
    port: this._options.port,
    storage: this._options.storage,
    username: this._options.username,
    password: this._options.password,
  };

  this._db = new DB(this._dbConfig);

  this._db
    .authenticate()
    .then(() => {
      done();
    })
    .catch((e) => done(new Error('sql triplestore authentication error ' + e.message)));
};

// Writes the results of the query to the given triple stream
SQLDatasource.prototype._executeQuery = async function (query, destination) {
  let q = {};
  if (query.subject) q.subject = query.subject;
  if (query.predicate) q.predicate = query.predicate;
  if (query.object) q.object = query.object;

  let countAll = (!q.subject && !q.predicate && !q.object);
  // console.log(query, countAll);

  // let results = this._countResults.get(query); console.log(results) if
  // (!results) {   results = await this._db.get(q, query.limit, query.offset);
  // this._countResults.set(query, results); }
  let results = await this._db.get(q, query.limit, query.offset);

  results.forEach(r => {
    destination._push(r);
  });
  let resultsCount = results.length;
  this._getPatternCount(q, countAll, (totalCount) => {
    destination.setProperty('metadata', {
      totalCount: totalCount,
      hasExactCount: resultsCount,
    });
    destination.close();
  });

  // .catch((e) => destination.emit('error', new Error(`Could not query
  // ${this._dbConfig.dialect} database ${this._dbConfig.database}`)));
};

SQLDatasource.prototype._getPatternCount = async function (query, countAll, cb) {
  if (countAll) cb(31007315);
  let count;
  // count = this._countCache.get(query);
  // console.log('count = ', count)

  // if (count) cb(count);
  // else {
    // console.log('no cache!', query)
    // console.log(this._countCache)

  count = await this._db.count(query);
  // this._countCache.set(query, count);
  // console.log('after set - get', this._countCache.get(query))
  // console.log('after set - cahche', this._countCache = '\n')
  cb(count);
  // }
};

module.exports = SQLDatasource;
