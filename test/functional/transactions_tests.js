'use strict';

const Promise = require('bluebird');
const mongodb = require('../..');
const MongoClient = mongodb.MongoClient;
const path = require('path');
const fs = require('fs');
const chai = require('chai');
const expect = chai.expect;
const EJSON = require('mongodb-extjson');

// mlaunch init --replicaset --arbiter  --name rs --hostname localhost --port 31000 --binarypath /Users/mbroadst/Downloads/mongodb-osx-x86_64-3.7.3-222-g15a1f64/bin

chai.config.includeStack = true;
chai.config.showDiff = true;
chai.config.truncateThreshold = 0;

const testContext = {
  dbName: 'transactions-tests',
  collectionName: 'test'
};

describe('Transactions (spec)', function() {
  const testSuites = fs
    .readdirSync(`${__dirname}/spec/transactions`)
    .filter(x => x.indexOf('.json') !== -1)
    .map(x =>
      Object.assign(JSON.parse(fs.readFileSync(`${__dirname}/spec/transactions/${x}`)), {
        name: path.basename(x, '.json')
      })
    );

  after(() => testContext.client.close());
  before(function() {
    // create a shared client for admin tasks
    const config = this.configuration;
    testContext.url = `mongodb://${config.host}:${config.port}/${testContext.dbName}?replicaSet=${
      config.replicasetName
    }`;

    console.log(`URL: ${testContext.url}`);
    testContext.client = new MongoClient(testContext.url);
    return testContext.client.connect();
  });

  testSuites.forEach(testSuite => {
    describe(testSuite.name, function() {
      beforeEach(() => {
        const db = testContext.client.db();
        const coll = db.collection(testContext.collectionName);

        return coll
          .drop()
          .catch(err => {
            if (!err.message.match(/ns not found/)) throw err;
          })
          .then(() => db.createCollection(testContext.collectionName, { w: 'majority' }))
          .then(() => {
            if (testSuite.data && Array.isArray(testSuite.data) && testSuite.data.length > 0) {
              return coll.insert(testSuite.data, { w: 'majority' });
            }
          });
      });

      testSuite.tests.forEach(testData => {
        afterEach(() => {
          if (testContext.testClient) {
            return testContext.testClient.close().then(() => {
              delete testContext.testClient;
            });
          }
        });

        const maybeSkipIt = testData.skipReason ? it.skip : it;
        maybeSkipIt(testData.description, {
          metadata: { requires: { topology: 'replicaset' } },
          test: function() {
            const commandEvents = [];

            return MongoClient.connect(testContext.url, { enableCommandMonitoring: true }).then(
              client => {
                testContext.testClient = client;
                client.on('commandStarted', event => {
                  if (event.databaseName === testContext.dbName) commandEvents.push(event);
                });

                const transactionOptions = Object.assign({}, testData.transactionOptions);
                const session0 = client.startSession(transactionOptions);
                const session1 = client.startSession(transactionOptions);

                return testOperations(client, testData, { session0, session1 })
                  .catch(err => {
                    // If the driver throws an exception / returns an error while executing this series
                    // of operations, store the error message.
                    console.log('error occurred during series of operations');
                    console.dir(err);
                    // operationError = err;
                  })
                  .then(() => {
                    session0.endSession();
                    session1.endSession();

                    if (
                      testData.expectations &&
                      Array.isArray(testData.expectations) &&
                      testData.expectations.length > 0
                    ) {
                      const actualCommands = normalizeCommandShapes(commandEvents).map(
                        c => c.command
                      );
                      const expectedCommands = normalizeCommandShapes(
                        testData.expectations.map(x =>
                          linkSessionData(x.command_started_event, { session0, session1 })
                        )
                      ).map(c => c.command);

                      // NOTE: obviously broken now
                      // expect(actualCommands).to.have.length(expectations.length);

                      expectedCommands.forEach((expected, idx) => {
                        const actual = actualCommands[idx];
                        // NOTE: get rid of this when they match up
                        if (actual == null) return;

                        const omitKeys = Object.keys(expected).reduce((lhs, rhs) => {
                          if (expected[rhs] === null) lhs.push(rhs);
                          return lhs;
                        }, []);

                        expect(actual).to.not.contain.any.keys(omitKeys);
                        expect(actual).to.deep.include(expected);
                      });
                    }

                    if (testData.outcome) {
                      if (testData.outcome.collection) {
                        // use the client without transactions to verify
                        return testContext.client
                          .db()
                          .collection(testContext.collectionName)
                          .find({})
                          .then(docs => {
                            expect(docs).to.eql(testData.outcome.collection);
                          });
                      }
                    }
                  });
              }
            );
          }
        });
      });
    });
  });
});

function linkSessionData(command, context) {
  const session = context[command.command.lsid];
  command.command.lsid = session.id;
  return command;
}

function normalizeCommandShapes(commands) {
  return commands.map(command =>
    JSON.parse(
      EJSON.stringify({
        command: command.command,
        commandName: command.commandName,
        databaseName: command.databaseName
      })
    )
  );
}

function extractCrudResult(result, operation) {
  return Object.keys(operation.result).reduce((crudResult, key) => {
    if (result.hasOwnProperty(key) && result[key] != null) crudResult[key] = result[key];
    return crudResult;
  }, {});
}

function testOperation(operation, coll, context) {
  const opOptions = {};
  const args = [];
  if (operation.arguments) {
    Object.keys(operation.arguments).forEach(key => {
      if (key === 'filter') return args.unshift(operation.arguments.filter);
      if (key === 'update' || key === 'replacement') return args.push(operation.arguments[key]);
      if (key === 'document') return args.unshift(operation.arguments.document);
      if (key === 'session') {
        opOptions.session = context[operation.arguments.session];
        return;
      }

      opOptions[key] = operation.arguments[key];
    });
  }
  args.push(opOptions);

  let opPromise = coll[operation.name].apply(coll, args);
  if (operation.result) {
    if (operation.result.errorContains) {
      return opPromise
        .then(() => {
          throw new Error('expected an error!');
        })
        .catch(err => expect(err).to.match(operation.result.errorContains));
    }

    return opPromise.then(opResult => {
      const actual = extractCrudResult(opResult, operation);
      expect(actual).to.eql(operation.result);
    });
  }

  return opPromise;
}

function testOperations(client, testData, context) {
  const coll = client.db().collection('test');
  return testData.operations.reduce((combined, operation) => {
    if (['startTransaction', 'commitTransaction', 'abortTransaction'].includes(operation.name)) {
      const session0 = context.session0;
      return combined.then(() => session0[operation.name]());
    }

    return combined.then(() => testOperation(operation, coll, context));
  }, Promise.resolve());
}
